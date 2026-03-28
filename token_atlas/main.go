package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	apiBaseDefault   = "https://gptfreetoken.pony.indevs.in"
	claimCount       = 2
	claimCheckPeriod = 5 * time.Second
)

var terminalStatuses = map[string]bool{
	"completed": true,
	"failed":    true,
	"error":     true,
	"success":   true,
	"succeeded": true,
	"expired":   true,
}

type Config struct {
	APIBase             string
	SaveDir             string
	EnvFile             string
	SessionCookie       string
	APIKey              string
	TokenSaveDir        string
	PollInterval        time.Duration
	PollMaxAttempts     int
	DownloadConcurrency int
	PollConcurrency     int
	VerifySSL           bool
}

type APIClient struct {
	cfg            Config
	httpClient     *http.Client
	fallbackClient *http.Client
}

type HistoryStore struct {
	path string
	mu   sync.Mutex
}

type History struct {
	Requests []RequestRecord `json:"requests"`
}

type RequestRecord struct {
	RequestID   string         `json:"request_id,omitempty"`
	Status      string         `json:"status,omitempty"`
	QueueStatus string         `json:"queue_status,omitempty"`
	Queued      *bool          `json:"queued,omitempty"`
	Granted     *int           `json:"granted,omitempty"`
	Requested   *int           `json:"requested,omitempty"`
	Items       []ClaimItem    `json:"items,omitempty"`
	Tokens      []TokenRecord  `json:"tokens,omitempty"`
	CreatedAt   string         `json:"created_at,omitempty"`
	UpdatedAt   string         `json:"updated_at,omitempty"`
	Raw         map[string]any `json:"-"`
}

type ClaimItem struct {
	TokenID any `json:"token_id,omitempty"`
}

type TokenRecord struct {
	TokenID        any    `json:"token_id,omitempty"`
	Downloaded     bool   `json:"downloaded,omitempty"`
	SavedToLocal   bool   `json:"saved_to_local,omitempty"`
	Status         string `json:"status,omitempty"`
	DownloadedAt   string `json:"downloaded_at,omitempty"`
	SavedToLocalAt string `json:"saved_to_local_at,omitempty"`
	FileName       string `json:"file_name,omitempty"`
	FilePath       string `json:"file_path,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
}

type DownloadedTokenFile struct {
	FileName string `json:"file_name,omitempty"`
	FilePath string `json:"file_path,omitempty"`
}


type FetchJob struct {
	RequestID string
	Payload   map[string]any
}

type DownloadJob struct {
	RequestID string
	Items     []ClaimItem
}

type pipelineState struct {
	pollQueued     atomic.Int64
	downloadQueued atomic.Int64
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Error] 加载配置失败: %v\n", err)
		os.Exit(1)
	}

	app := NewApp(cfg)
	if err := app.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "[Error] %v\n", err)
		os.Exit(1)
	}
}

type App struct {
	cfg     Config
	client  *APIClient
	history *HistoryStore
	state   *pipelineState
}

func NewApp(cfg Config) *App {
	return &App{
		cfg:     cfg,
		client:  NewAPIClient(cfg),
		history: &HistoryStore{path: filepath.Join(cfg.SaveDir, "request_history.json")},
		state:   &pipelineState{},
	}
}

func (a *App) Run(ctx context.Context) error {
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("  Token Atlas Go 工具")
	fmt.Printf("  API Base: %s\n", a.cfg.APIBase)
	fmt.Printf("  Session Cookie: %s\n", yesNo(a.cfg.SessionCookie != ""))
	fmt.Printf("  API Key: %s\n", yesNo(a.cfg.APIKey != ""))
	fmt.Printf("  Save Dir: %s\n", a.cfg.SaveDir)
	fmt.Printf("  Poll Concurrency: %d\n", a.cfg.PollConcurrency)
	fmt.Printf("  Download Concurrency: %d\n", a.cfg.DownloadConcurrency)
	fmt.Println(strings.Repeat("=", 60))

	if _, _, err := a.fetchMeQuota(ctx, "[Startup /me]"); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pollQueue := make(chan FetchJob, 128)
	downloadQueue := make(chan DownloadJob, 128)
	queued := newQueuedSet()

	if err := a.enqueueRecoveredJobs(pollQueue, downloadQueue, queued); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.claimWorker(ctx, pollQueue, downloadQueue, queued)
	}()

	for i := 0; i < a.cfg.PollConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.pollWorker(ctx, pollQueue, downloadQueue, queued)
		}()
	}

	for i := 0; i < a.cfg.DownloadConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.downloadWorker(ctx, downloadQueue)
		}()
	}

	<-ctx.Done()
	close(pollQueue)
	close(downloadQueue)
	wg.Wait()
	return a.saveHistorySnapshotIfNeeded()
}

func (a *App) claimWorker(ctx context.Context, pollQueue chan<- FetchJob, downloadQueue chan<- DownloadJob, queued *queuedSet) {
	ticker := time.NewTicker(claimCheckPeriod)
	defer ticker.Stop()

	for {
		if err := a.enqueueRecoveredJobs(pollQueue, downloadQueue, queued); err != nil {
			fmt.Printf("[ClaimWorker] 恢复任务失败: %v\n", err)
		}

		idle, err := a.pipelineIdle(queued)
		if err != nil {
			fmt.Printf("[ClaimWorker] 检查流水线状态失败: %v\n", err)
		} else if idle {
			_, remaining, err := a.fetchMeQuota(ctx, "[ClaimWorker /me]")
			if err != nil {
				fmt.Printf("[ClaimWorker] 查询额度失败: %v\n", err)
			} else if remaining <= 0 {
				fmt.Println("[Info] 当前没有可申请额度，等待下一轮检查")
			} else if err := a.createNewClaimIfNeeded(ctx, pollQueue, queued, remaining); err != nil {
				fmt.Printf("[ClaimWorker] 发起 claim 失败: %v\n", err)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *App) pollWorker(ctx context.Context, pollQueue <-chan FetchJob, downloadQueue chan<- DownloadJob, queued *queuedSet) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-pollQueue:
			if !ok {
				return
			}
			a.state.pollQueued.Add(-1)
			func() {
				defer queued.Remove(job.RequestID)
				latestPayload := job.Payload
				latestStatus := normalizeRequestStatus(latestPayload)
				if latestStatus == "" {
					latestStatus = "unknown"
				}

				if !terminalStatuses[latestStatus] {
					if a.cfg.APIKey == "" {
						fmt.Printf("[Warning] request_id=%s 缺少 API_KEY，无法轮询状态\n", job.RequestID)
						return
					}
					fmt.Printf("[Poll] request_id=%s 开始轮询\n", job.RequestID)
					for attempt := 1; attempt <= a.cfg.PollMaxAttempts; attempt++ {
						select {
						case <-ctx.Done():
							return
						case <-time.After(a.cfg.PollInterval):
						}
						queryResult, err := a.client.GetAPIClaimStatus(ctx, job.RequestID)
						if err != nil {
							fmt.Printf("[Poll] request_id=%s 第 %d 次查询失败: %v\n", job.RequestID, attempt, err)
							continue
						}
						latestPayload = queryResult
						latestStatus = normalizeRequestStatus(queryResult)
						fmt.Printf("[Poll] request_id=%s 第 %d 次状态=%s\n", job.RequestID, attempt, latestStatus)
						_ = saveJSONFixed(a.cfg.SaveDir, queryResult, fmt.Sprintf("request_%s", job.RequestID))
						_ = a.history.UpdateRequestStatus(job.RequestID, queryResult)
						if terminalStatuses[latestStatus] {
							fmt.Printf("[Poll] request_id=%s 已完成\n", job.RequestID)
							break
						}
					}
					if !terminalStatuses[latestStatus] {
						fmt.Printf("[Warning] request_id=%s 轮询超时\n", job.RequestID)
						return
					}
				} else {
					_ = a.history.UpdateRequestStatus(job.RequestID, latestPayload)
				}

				record, err := a.history.GetRequestRecord(job.RequestID)
				if err != nil {
					fmt.Printf("[History] 获取 request_id=%s 失败: %v\n", job.RequestID, err)
					return
				}
				items := extractItems(record, latestPayload)
				if len(items) > 0 {
					fmt.Printf("[Queue] request_id=%s 加入下载队列，items=%d\n", job.RequestID, len(items))
				} else {
					fmt.Printf("[Queue] request_id=%s 无可下载项\n", job.RequestID)
				}
				a.state.downloadQueued.Add(1)
				downloadQueue <- DownloadJob{RequestID: job.RequestID, Items: items}
			}()
		}
	}
}

func (a *App) downloadWorker(ctx context.Context, downloadQueue <-chan DownloadJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-downloadQueue:
			if !ok {
				return
			}
			a.state.downloadQueued.Add(-1)
			if len(job.Items) == 0 {
				fullyProcessed, err := a.history.RequestFullyProcessed(a.cfg.SaveDir, a.cfg.TokenSaveDir, job.RequestID)
				if err == nil && fullyProcessed {
					_ = a.history.RemoveRequestRecord(job.RequestID)
				}
				continue
			}

			for _, item := range job.Items {
				tokenID := stringifyTokenID(item.TokenID)
				if tokenID == "" {
					continue
				}

				tokenRecord, _ := a.history.GetTokenRecord(job.RequestID, tokenID)
				tokenFilePath := resolveTokenFilePath(a.cfg.SaveDir, tokenRecord)
				alreadyDownloaded := tokenRecord != nil && tokenRecord.Downloaded && tokenFilePath != ""
				if alreadyDownloaded {
					fmt.Printf("[Skip] request_id=%s, token=%s 已下载，跳过重复下载\n", job.RequestID, tokenID)
				} else {
					tokenData, err := a.client.DownloadToken(ctx, tokenID)
					if err != nil {
						fmt.Printf("[Download] request_id=%s, token=%s 下载失败，保留历史等待下次续跑: %v\n", job.RequestID, tokenID, err)
						continue
					}
					downloadedFile := extractDownloadedTokenFile(tokenData)
					desiredFileName := normalizeJSONFileName(downloadedFile.FileName, fmt.Sprintf("token_%s.json", tokenID))
					savedPath, err := saveJSONWithName(a.cfg.SaveDir, tokenData, desiredFileName)
					if err != nil {
						fmt.Printf("[Save] request_id=%s, token=%s 写文件失败: %v\n", job.RequestID, tokenID, err)
						continue
					}
					_ = a.history.UpdateTokenRecord(job.RequestID, tokenID, TokenRecord{
						TokenID:      tokenID,
						Downloaded:   true,
						Status:       "downloaded",
						DownloadedAt: nowString(),
						FileName:     filepath.Base(savedPath),
						FilePath:     savedPath,
					})
					tokenFilePath = savedPath
				}

				tokenRecord, _ = a.history.GetTokenRecord(job.RequestID, tokenID)
				alreadySaved := tokenRecord != nil && tokenRecord.SavedToLocal
				if a.cfg.TokenSaveDir != "" && tokenFilePath != "" {
					if alreadySaved {
						fmt.Printf("[Skip] request_id=%s, token=%s 已保存到本地\n", job.RequestID, tokenID)
					} else {
						saved, err := saveToLocal(tokenFilePath, a.cfg.TokenSaveDir)
						if err != nil {
							fmt.Printf("[Save] request_id=%s, token=%s 保存失败，保留历史等待重试: %v\n", job.RequestID, tokenID, err)
						} else if saved {
							_ = a.history.UpdateTokenRecord(job.RequestID, tokenID, TokenRecord{
								TokenID:        tokenID,
								SavedToLocal:   true,
								SavedToLocalAt: nowString(),
								Status:         "saved",
							})
						}
					}
				}
			}

			fullyProcessed, err := a.history.RequestFullyProcessed(a.cfg.SaveDir, a.cfg.TokenSaveDir, job.RequestID)
			if err == nil && fullyProcessed {
				_ = a.history.RemoveRequestRecord(job.RequestID)
			}
		}
	}
}

func (a *App) enqueueRecoveredJobs(pollQueue chan<- FetchJob, downloadQueue chan<- DownloadJob, queued *queuedSet) error {
	pending, err := a.history.FindPendingRequests()
	if err != nil {
		return err
	}
	downloadable, err := a.history.FindDownloadableRequests()
	if err != nil {
		return err
	}

	for _, req := range pending {
		if req.RequestID == "" || queued.Has(req.RequestID) {
			continue
		}
		fmt.Printf("[Resume] 恢复等待中的请求: request_id=%s, status=%s\n", req.RequestID, req.Status)
		queued.Add(req.RequestID)
		a.state.pollQueued.Add(1)
		pollQueue <- FetchJob{RequestID: req.RequestID, Payload: req.ToMap()}
	}

	for _, req := range downloadable {
		if req.RequestID == "" || len(req.Items) == 0 {
			continue
		}
		fullyProcessed, err := a.history.RequestFullyProcessed(a.cfg.SaveDir, a.cfg.TokenSaveDir, req.RequestID)
		if err != nil {
			return err
		}
		if fullyProcessed {
			continue
		}
		fmt.Printf("[Resume] 恢复可下载请求: request_id=%s, items=%d\n", req.RequestID, len(req.Items))
		a.state.downloadQueued.Add(1)
		downloadQueue <- DownloadJob{RequestID: req.RequestID, Items: req.Items}
	}
	return nil
}

func (a *App) pipelineIdle(queued *queuedSet) (bool, error) {
	pending, err := a.history.FindPendingRequests()
	if err != nil {
		return false, err
	}
	if len(pending) > 0 {
		return false, nil
	}
	downloadable, err := a.history.FindDownloadableRequests()
	if err != nil {
		return false, err
	}
	for _, req := range downloadable {
		fullyProcessed, err := a.history.RequestFullyProcessed(a.cfg.SaveDir, a.cfg.TokenSaveDir, req.RequestID)
		if err != nil {
			return false, err
		}
		if !fullyProcessed {
			return false, nil
		}
	}
	return queued.Len() == 0 && a.state.pollQueued.Load() == 0 && a.state.downloadQueued.Load() == 0, nil
}

func (a *App) fetchMeQuota(ctx context.Context, prefix string) (map[string]any, int, error) {
	if a.cfg.SessionCookie == "" && a.cfg.APIKey == "" {
		fmt.Println("[Warning] 未配置 TOKEN_ATLAS_SESSION 或 TOKEN_ATLAS_API_KEY，跳过 /me 额度检查")
		return map[string]any{}, 0, nil
	}
	meResult, err := a.client.GetMe(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("获取用户信息失败: %w", err)
	}
	_, _ = saveJSON(a.cfg.SaveDir, meResult, "me")
	_, _, remaining := extractMeQuota(meResult)
	fmt.Printf("%s 额度 remaining=%d\n", prefix, remaining)
	return meResult, remaining, nil
}

func (a *App) createNewClaimIfNeeded(ctx context.Context, pollQueue chan<- FetchJob, queued *queuedSet, remaining int) error {
	if a.cfg.SessionCookie == "" {
		return errors.New("请在 .env 中配置 TOKEN_ATLAS_SESSION，用于调用 /me/claim 发起申请")
	}
	if remaining <= 0 {
		fmt.Println("[Info] 当前没有可申请额度，跳过本次申请")
		return nil
	}
	count := claimCount
	if remaining < count {
		count = remaining
	}
	fmt.Printf("[Claim] 准备申请 %d 个\n", count)
	claimResult, err := a.client.SessionClaim(ctx, count)
	if err != nil {
		return fmt.Errorf("领取请求失败: %w", err)
	}
	_, _ = saveJSON(a.cfg.SaveDir, claimResult, "claim")
	requestID, _ := claimResult["request_id"].(string)
	if requestID == "" {
		return errors.New("领取响应中缺少 request_id")
	}
	fmt.Printf("[Claim] 已创建 request_id=%s\n", requestID)
	_ = a.history.UpdateRequestStatus(requestID, claimResult)
	if !queued.Has(requestID) {
		queued.Add(requestID)
		a.state.pollQueued.Add(1)
		pollQueue <- FetchJob{RequestID: requestID, Payload: claimResult}
	}
	return nil
}

func (a *App) saveHistorySnapshotIfNeeded() error {
	if a.cfg.TokenSaveDir == "" {
		return nil
	}
	historyPath := filepath.Join(a.cfg.SaveDir, "request_history.json")
	if _, err := os.Stat(historyPath); err != nil {
		return nil
	}
	_, err := saveToLocal(historyPath, a.cfg.TokenSaveDir)
	return err
}

func loadConfig() (Config, error) {
	exePath, err := os.Executable()
	if err != nil {
		return Config{}, err
	}
	exeDir := filepath.Dir(exePath)
	saveDir := exeDir
	envFile := filepath.Join(exeDir, ".env")
	if _, err := os.Stat(envFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return Config{}, err
		}
		if wd, wdErr := os.Getwd(); wdErr == nil {
			fallbackEnv := filepath.Join(wd, ".env")
			if _, statErr := os.Stat(fallbackEnv); statErr == nil {
				envFile = fallbackEnv
				saveDir = wd
			}
		}
	}
	env, err := loadEnvFile(envFile)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		APIBase:             firstNonEmpty(env["TOKEN_ATLAS_API_BASE"], apiBaseDefault),
		SaveDir:             saveDir,
		EnvFile:             envFile,
		SessionCookie:       env["TOKEN_ATLAS_SESSION"],
		APIKey:              env["TOKEN_ATLAS_API_KEY"],
		TokenSaveDir:        env["TOKEN_SAVE_DIR"],
		PollInterval:        time.Duration(getInt(env, "TOKEN_ATLAS_POLL_INTERVAL_SECONDS", 30)) * time.Second,
		PollMaxAttempts:     getInt(env, "TOKEN_ATLAS_POLL_MAX_ATTEMPTS", 20),
		DownloadConcurrency: max(1, getInt(env, "TOKEN_ATLAS_DOWNLOAD_CONCURRENCY", 3)),
		PollConcurrency:     max(1, getInt(env, "TOKEN_ATLAS_POLL_CONCURRENCY", 2)),
		VerifySSL:           getBool(env, "TOKEN_ATLAS_VERIFY_SSL", true),
	}
	return cfg, nil
}

func loadEnvFile(path string) (map[string]string, error) {
	result := map[string]string{}
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result, nil
		}
		return nil, err
	}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return result, nil
}

func shortBody(data []byte, limit int) string {
	if len(data) == 0 {
		return ""
	}
	text := strings.ReplaceAll(string(data), "\n", "\\n")
	text = strings.ReplaceAll(text, "\r", "\\r")
	if len(text) > limit {
		return text[:limit] + "..."
	}
	return text
}

func maskedHeaders(headers map[string]string) map[string]string {
	masked := make(map[string]string, len(headers))
	for k, v := range headers {
		switch strings.ToLower(k) {
		case "cookie":
			if len(v) > 24 {
				masked[k] = v[:24] + "...(masked)"
			} else if v != "" {
				masked[k] = "(masked)"
			} else {
				masked[k] = v
			}
		case "x-api-key", "authorization":
			if v != "" {
				masked[k] = "(masked)"
			} else {
				masked[k] = v
			}
		default:
			masked[k] = v
		}
	}
	return masked
}

func classifyNetworkError(err error) string {
	if err == nil {
		return ""
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout"
		}
		return "net_error"
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return "connection_reset"
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "connection reset by peer"):
		return "connection_reset"
	case strings.Contains(message, "tls"):
		return "tls_error"
	case strings.Contains(message, "no such host"):
		return "dns_error"
	case strings.Contains(message, "eof"):
		return "unexpected_eof"
	default:
		return "request_error"
	}
}

func NewAPIClient(cfg Config) *APIClient {
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !cfg.VerifySSL}}
	fallbackTransport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}}
	return &APIClient{
		cfg:            cfg,
		httpClient:     &http.Client{Timeout: 30 * time.Second, Transport: transport},
		fallbackClient: &http.Client{Timeout: 30 * time.Second, Transport: fallbackTransport},
	}
}

func (c *APIClient) requestJSON(ctx context.Context, method, endpoint string, body map[string]any, useAPIKey, useSession bool) (map[string]any, error) {
	url := strings.TrimRight(c.cfg.APIBase, "/") + endpoint
	headers := map[string]string{
		"Content-Type":    "application/json",
		"User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Accept":          "application/json",
		"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
		"Referer":         c.cfg.APIBase,
	}
	if useAPIKey && c.cfg.APIKey != "" {
		headers["X-API-Key"] = c.cfg.APIKey
	}
	if useSession && c.cfg.SessionCookie != "" {
		headers["Cookie"] = fmt.Sprintf("token_atlas_session=%s", c.cfg.SessionCookie)
	}
	respBody, status, err := c.do(ctx, method, url, headers, body, 30*time.Second, "[Warning] SSL 证书校验失败，尝试关闭校验后重试")
	if err != nil {
		fmt.Printf("[HTTP Debug] request failed method=%s endpoint=%s timeout=%s apiKey=%t session=%t verifySSL=%t headers=%v err=%T %v kind=%s\n",
			method,
			endpoint,
			30*time.Second,
			useAPIKey,
			useSession,
			c.cfg.VerifySSL,
			maskedHeaders(headers),
			err,
			err,
			classifyNetworkError(err),
		)
		if body != nil {
			payload, _ := json.Marshal(body)
			fmt.Printf("[HTTP Debug] request body endpoint=%s body=%s\n", endpoint, shortBody(payload, 300))
		}
		return nil, err
	}
	fmt.Printf("[HTTP] %s %s -> %d\n", method, endpoint, status)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("响应 JSON 解析失败: %w", err)
	}
	return result, nil
}

func (c *APIClient) do(ctx context.Context, method, url string, headers map[string]string, body map[string]any, timeout time.Duration, fallbackMessage string) ([]byte, int, error) {
	client := c.httpClient
	client.Timeout = timeout
	respBody, status, err := doRequest(ctx, client, method, url, headers, body)
	if err == nil {
		return respBody, status, nil
	}
	fmt.Printf("[HTTP Debug] primary client failed method=%s url=%s err=%T %v kind=%s\n", method, url, err, err, classifyNetworkError(err))
	var urlErr *urlError
	if errors.As(err, &urlErr) && c.cfg.VerifySSL {
		fmt.Println(fallbackMessage)
		c.fallbackClient.Timeout = timeout
		respBody, status, fallbackErr := doRequest(ctx, c.fallbackClient, method, url, headers, body)
		if fallbackErr == nil {
			fmt.Printf("[HTTP] %s %s -> %d (SSL verify disabled)\n", method, strings.TrimPrefix(url, c.cfg.APIBase), status)
			return respBody, status, nil
		}
		fmt.Printf("[HTTP Debug] fallback client failed method=%s url=%s err=%T %v kind=%s\n", method, url, fallbackErr, fallbackErr, classifyNetworkError(fallbackErr))
		return nil, 0, fallbackErr
	}
	return nil, 0, err
}


type urlError struct{ err error }

func (e *urlError) Error() string { return e.err.Error() }
func (e *urlError) Unwrap() error { return e.err }

func doRequest(ctx context.Context, client *http.Client, method, url string, headers map[string]string, body map[string]any) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		reader = strings.NewReader(string(payload))
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("[HTTP Debug] client.Do failed method=%s host=%s path=%s elapsed=%s err=%T %v kind=%s\n", method, req.URL.Host, req.URL.Path, time.Since(start), err, err, classifyNetworkError(err))
		if strings.Contains(strings.ToLower(err.Error()), "certificate") || strings.Contains(strings.ToLower(err.Error()), "x509") {
			return nil, 0, &urlError{err: err}
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("[HTTP Debug] read body failed method=%s path=%s elapsed=%s err=%T %v kind=%s\n", method, req.URL.Path, time.Since(start), err, err, classifyNetworkError(err))
		return nil, 0, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Printf("[HTTP Debug] non-2xx response method=%s path=%s status=%d elapsed=%s server=%s cfRay=%s body=%s\n", method, req.URL.Path, resp.StatusCode, time.Since(start), resp.Header.Get("Server"), resp.Header.Get("Cf-Ray"), shortBody(respBody, 500))
		return nil, resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, resp.StatusCode, nil
}


func (c *APIClient) GetMe(ctx context.Context) (map[string]any, error) {
	if c.cfg.SessionCookie != "" {
		fmt.Println("[Me] 尝试使用 Session 调用 /me")
		result, err := c.requestJSON(ctx, http.MethodGet, "/me", nil, false, true)
		if err == nil {
			return result, nil
		}
		fmt.Printf("[Me] Session 调用 /me 失败: %T %v\n", err, err)
		fmt.Println("[Me] Session 获取失败，尝试 API Key")
	}
	if c.cfg.APIKey == "" {
		return nil, errors.New("未配置 API Key")
	}
	fmt.Println("[Me] 尝试使用 API Key 调用 /me")
	return c.requestJSON(ctx, http.MethodGet, "/me", nil, true, false)
}


func (c *APIClient) SessionClaim(ctx context.Context, count int) (map[string]any, error) {
	return c.requestJSON(ctx, http.MethodPost, "/me/claim", map[string]any{"count": count}, false, true)
}

func (c *APIClient) GetAPIClaimStatus(ctx context.Context, requestID string) (map[string]any, error) {
	return c.requestJSON(ctx, http.MethodGet, "/api/claims/"+requestID, nil, true, false)
}

func (c *APIClient) DownloadToken(ctx context.Context, tokenID string) (map[string]any, error) {
	return c.requestJSON(ctx, http.MethodGet, "/api/download/"+tokenID, nil, true, false)
}

func (r RequestRecord) ToMap() map[string]any {
	data := map[string]any{}
	if r.RequestID != "" {
		data["request_id"] = r.RequestID
	}
	if r.Status != "" {
		data["status"] = r.Status
	}
	if r.QueueStatus != "" {
		data["queue_status"] = r.QueueStatus
	}
	if r.Queued != nil {
		data["queued"] = *r.Queued
	}
	if r.Granted != nil {
		data["granted"] = *r.Granted
	}
	if r.Requested != nil {
		data["requested"] = *r.Requested
	}
	if len(r.Items) > 0 {
		items := make([]any, 0, len(r.Items))
		for _, item := range r.Items {
			items = append(items, map[string]any{"token_id": item.TokenID})
		}
		data["items"] = items
	}
	return data
}

func (h *HistoryStore) Load() (History, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	content, err := os.ReadFile(h.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return History{Requests: []RequestRecord{}}, nil
		}
		return History{}, err
	}
	var history History
	if err := json.Unmarshal(content, &history); err != nil {
		return History{}, err
	}
	if history.Requests == nil {
		history.Requests = []RequestRecord{}
	}
	return history, nil
}

func (h *HistoryStore) Save(history History) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	content, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(h.path, content, 0o644)
}

func (h *HistoryStore) UpdateRequestStatus(requestID string, data map[string]any) error {
	history, err := h.Load()
	if err != nil {
		return err
	}
	found := false
	for i := range history.Requests {
		if history.Requests[i].RequestID == requestID {
			mergeRequestRecord(&history.Requests[i], data)
			history.Requests[i].Status = normalizeRequestStatus(history.Requests[i].ToMap())
			history.Requests[i].UpdatedAt = nowString()
			found = true
			break
		}
	}
	if !found {
		record := mapToRequestRecord(data)
		record.RequestID = requestID
		record.Status = normalizeRequestStatus(record.ToMap())
		record.CreatedAt = nowString()
		record.UpdatedAt = nowString()
		history.Requests = append(history.Requests, record)
	}
	return h.Save(history)
}

func (h *HistoryStore) RemoveRequestRecord(requestID string) error {
	history, err := h.Load()
	if err != nil {
		return err
	}
	filtered := history.Requests[:0]
	for _, req := range history.Requests {
		if req.RequestID != requestID {
			filtered = append(filtered, req)
		}
	}
	history.Requests = filtered
	return h.Save(history)
}

func (h *HistoryStore) FindPendingRequests() ([]RequestRecord, error) {
	history, err := h.Load()
	if err != nil {
		return nil, err
	}
	var out []RequestRecord
	for _, req := range history.Requests {
		status := normalizeRequestStatus(req.ToMap())
		if !terminalStatuses[status] {
			req.Status = status
			out = append(out, req)
		}
	}
	return out, nil
}

func (h *HistoryStore) FindDownloadableRequests() ([]RequestRecord, error) {
	history, err := h.Load()
	if err != nil {
		return nil, err
	}
	var out []RequestRecord
	for _, req := range history.Requests {
		status := normalizeRequestStatus(req.ToMap())
		if terminalStatuses[status] {
			req.Status = status
			out = append(out, req)
		}
	}
	return out, nil
}

func (h *HistoryStore) GetRequestRecord(requestID string) (*RequestRecord, error) {
	history, err := h.Load()
	if err != nil {
		return nil, err
	}
	for _, req := range history.Requests {
		if req.RequestID == requestID {
			copyReq := req
			return &copyReq, nil
		}
	}
	return nil, nil
}

func (h *HistoryStore) GetTokenRecord(requestID, tokenID string) (*TokenRecord, error) {
	req, err := h.GetRequestRecord(requestID)
	if err != nil || req == nil {
		return nil, err
	}
	for _, token := range req.Tokens {
		if stringifyTokenID(token.TokenID) == tokenID {
			copyToken := token
			return &copyToken, nil
		}
	}
	return nil, nil
}

func (h *HistoryStore) UpdateTokenRecord(requestID, tokenID string, data TokenRecord) error {
	history, err := h.Load()
	if err != nil {
		return err
	}
	for i := range history.Requests {
		if history.Requests[i].RequestID != requestID {
			continue
		}
		for j := range history.Requests[i].Tokens {
			if stringifyTokenID(history.Requests[i].Tokens[j].TokenID) == tokenID {
				mergeTokenRecord(&history.Requests[i].Tokens[j], data)
				history.Requests[i].Tokens[j].TokenID = tokenID
				history.Requests[i].Tokens[j].UpdatedAt = nowString()
				history.Requests[i].UpdatedAt = nowString()
				return h.Save(history)
			}
		}
		data.TokenID = tokenID
		if data.CreatedAt == "" {
			data.CreatedAt = nowString()
		}
		data.UpdatedAt = nowString()
		history.Requests[i].Tokens = append(history.Requests[i].Tokens, data)
		history.Requests[i].UpdatedAt = nowString()
		return h.Save(history)
	}
	return nil
}

func (h *HistoryStore) RequestFullyProcessed(saveDir, tokenSaveDir, requestID string) (bool, error) {
	req, err := h.GetRequestRecord(requestID)
	if err != nil || req == nil {
		return false, err
	}
	status := normalizeRequestStatus(req.ToMap())
	if !terminalStatuses[status] {
		return false, nil
	}
	if len(req.Items) == 0 {
		return true, nil
	}
	for _, item := range req.Items {
		tokenID := stringifyTokenID(item.TokenID)
		if tokenID == "" {
			return false, nil
		}
		tokenRecord, err := h.GetTokenRecord(requestID, tokenID)
		if err != nil || tokenRecord == nil {
			return false, err
		}
		if !tokenRecord.Downloaded {
			return false, nil
		}
		if tokenSaveDir != "" && !tokenRecord.SavedToLocal {
			return false, nil
		}
		if resolveTokenFilePath(saveDir, tokenRecord) == "" {
			return false, nil
		}
	}
	return true, nil
}

func mapToRequestRecord(data map[string]any) RequestRecord {
	record := RequestRecord{}
	mergeRequestRecord(&record, data)
	return record
}

func mergeRequestRecord(target *RequestRecord, data map[string]any) {
	if v, ok := data["request_id"].(string); ok {
		target.RequestID = v
	}
	if v, ok := data["status"].(string); ok {
		target.Status = v
	}
	if v, ok := data["queue_status"].(string); ok {
		target.QueueStatus = v
	}
	if v, ok := data["queued"].(bool); ok {
		target.Queued = &v
	}
	if v, ok := asInt(data["granted"]); ok {
		target.Granted = &v
	}
	if v, ok := asInt(data["requested"]); ok {
		target.Requested = &v
	}
	if items, ok := data["items"].([]any); ok {
		target.Items = make([]ClaimItem, 0, len(items))
		for _, item := range items {
			if m, ok := item.(map[string]any); ok {
				target.Items = append(target.Items, ClaimItem{TokenID: m["token_id"]})
			}
		}
	}
}

func mergeTokenRecord(target *TokenRecord, data TokenRecord) {
	if data.TokenID != nil {
		target.TokenID = data.TokenID
	}
	if data.Downloaded {
		target.Downloaded = true
	}
	if data.SavedToLocal {
		target.SavedToLocal = true
	}
	if data.Status != "" {
		target.Status = data.Status
	}
	if data.DownloadedAt != "" {
		target.DownloadedAt = data.DownloadedAt
	}
	if data.SavedToLocalAt != "" {
		target.SavedToLocalAt = data.SavedToLocalAt
	}
	if data.FileName != "" {
		target.FileName = data.FileName
	}
	if data.FilePath != "" {
		target.FilePath = data.FilePath
	}
	if data.CreatedAt != "" {
		target.CreatedAt = data.CreatedAt
	}
	if data.UpdatedAt != "" {
		target.UpdatedAt = data.UpdatedAt
	}
}

func normalizeRequestStatus(data map[string]any) string {
	if queueStatus, ok := data["queue_status"].(string); ok {
		queueStatus = strings.ToLower(strings.TrimSpace(queueStatus))
		if queueStatus == "succeeded" {
			return "completed"
		}
		if queueStatus != "" {
			return queueStatus
		}
	}
	if status, ok := data["status"].(string); ok {
		status = strings.ToLower(strings.TrimSpace(status))
		if status != "" {
			return status
		}
	}
	if queued, ok := data["queued"].(bool); ok && queued {
		return "queued_waiting"
	}
	if items, ok := data["items"].([]any); ok && len(items) > 0 {
		return "completed"
	}
	granted, gOK := asInt(data["granted"])
	requested, rOK := asInt(data["requested"])
	if gOK && rOK && requested > 0 && granted >= requested {
		return "completed"
	}
	return "unknown"
}

func extractItems(record *RequestRecord, payload map[string]any) []ClaimItem {
	if record != nil && len(record.Items) > 0 {
		return record.Items
	}
	if items, ok := payload["items"].([]any); ok {
		out := make([]ClaimItem, 0, len(items))
		for _, item := range items {
			if m, ok := item.(map[string]any); ok {
				out = append(out, ClaimItem{TokenID: m["token_id"]})
			}
		}
		return out
	}
	return nil
}

func extractMeQuota(data map[string]any) (int, int, int) {
	quotaMap, _ := data["quota"].(map[string]any)
	used, _ := asInt(quotaMap["used"])
	limit, _ := asInt(quotaMap["limit"])
	remaining, _ := asInt(quotaMap["remaining"])
	return max(0, used), max(0, limit), max(0, remaining)
}

func extractDownloadedTokenFile(data map[string]any) DownloadedTokenFile {
	result := DownloadedTokenFile{}
	if fileName, ok := data["file_name"].(string); ok {
		result.FileName = strings.TrimSpace(fileName)
	}
	if filePath, ok := data["file_path"].(string); ok {
		result.FilePath = strings.TrimSpace(filePath)
	}
	return result
}

func normalizeJSONFileName(fileName string, fallback string) string {
	name := strings.TrimSpace(fileName)
	if name == "" {
		return fallback
	}
	name = filepath.Base(name)
	if name == "." || name == string(filepath.Separator) || name == "" {
		return fallback
	}
	return name
}

func saveJSONWithName(saveDir string, data map[string]any, fileName string) (string, error) {
	path := filepath.Join(saveDir, normalizeJSONFileName(fileName, fmt.Sprintf("token_%s.json", time.Now().Format("20060102_150405"))))
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return "", err
	}
	// fmt.Printf("[Saved] %s\n", path)
	return path, nil
}

func saveJSON(saveDir string, data map[string]any, filename string) (string, error) {
	return saveJSONWithName(saveDir, data, fmt.Sprintf("%s_%s.json", filename, time.Now().Format("20060102_150405")))
}

func saveJSONFixed(saveDir string, data map[string]any, filename string) error {
	path := filepath.Join(saveDir, filename+".json")
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return err
	}
	// fmt.Printf("[Saved] %s\n", path)
	return nil
}

func saveToLocal(srcPath, targetDir string) (bool, error) {
	if targetDir == "" {
		fmt.Println("[Save] ⚠️ 未配置 TOKEN_SAVE_DIR，跳过保存")
		return false, nil
	}
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return false, err
	}
	destPath := filepath.Join(targetDir, filepath.Base(srcPath))
	in, err := os.Open(srcPath)
	if err != nil {
		return false, err
	}
	defer in.Close()
	out, err := os.Create(destPath)
	if err != nil {
		return false, err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return false, err
	}
	if err := os.Chmod(destPath, srcInfo.Mode()); err != nil {
		return false, err
	}
	fmt.Printf("[Save] ✅ %s 已保存到 %s\n", filepath.Base(srcPath), destPath)
	return true, nil
}

func resolveTokenFilePath(saveDir string, tokenRecord *TokenRecord) string {
	if tokenRecord == nil {
		return ""
	}
	if tokenRecord.FileName != "" {
		path := filepath.Join(saveDir, tokenRecord.FileName)
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	if tokenRecord.FilePath != "" {
		path := tokenRecord.FilePath
		if !filepath.IsAbs(path) {
			path = filepath.Join(saveDir, filepath.Base(path))
		}
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}

func stringifyTokenID(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case float64:
		return strconv.FormatInt(int64(value), 10)
	case int:
		return strconv.Itoa(value)
	case int64:
		return strconv.FormatInt(value, 10)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func getInt(env map[string]string, key string, fallback int) int {
	if value, ok := env[key]; ok && value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func getBool(env map[string]string, key string, fallback bool) bool {
	value, ok := env[key]
	if !ok || value == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "false", "no", "off":
		return false
	case "1", "true", "yes", "on":
		return true
	default:
		return fallback
	}
}

func asInt(v any) (int, bool) {
	switch value := v.(type) {
	case int:
		return value, true
	case int64:
		return int(value), true
	case float64:
		return int(value), true
	case json.Number:
		parsed, err := value.Int64()
		return int(parsed), err == nil
	case string:
		parsed, err := strconv.Atoi(value)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func nowString() string { return time.Now().Format("2006-01-02 15:04:05") }
func yesNo(ok bool) string {
	if ok {
		return "已配置"
	}
	return "未配置"
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type queuedSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newQueuedSet() *queuedSet { return &queuedSet{m: map[string]struct{}{}} }
func (q *queuedSet) Add(id string) {
	q.mu.Lock()
	q.m[id] = struct{}{}
	q.mu.Unlock()
}
func (q *queuedSet) Remove(id string) {
	q.mu.Lock()
	delete(q.m, id)
	q.mu.Unlock()
}
func (q *queuedSet) Has(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.m[id]
	return ok
}
func (q *queuedSet) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.m)
}
