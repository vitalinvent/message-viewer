package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"embed"

	"github.com/nats-io/nats.go"
)

//go:embed web/index.html
var webFiles embed.FS

const (
	defaultNATSURL  = "nats://127.0.0.1:4222"
	maxRequestBody  = 1 << 20 // 1 MiB
	maxMessageBytes = 1 << 20
	defaultMaxScan  = 100000
	absoluteMaxScan = 500000
)

type QueryRequest struct {
	Source         string `json:"source"`
	NATSURL        string `json:"natsURL"`
	User           string `json:"user"`
	Password       string `json:"password"`
	Token          string `json:"token"`
	Stream         string `json:"stream"`
	Subject        string `json:"subject"`
	Start          string `json:"start"`
	End            string `json:"end"`
	Limit          int    `json:"limit"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
	MaxScan        int    `json:"maxScan"`
}

type ViewMessage struct {
	Time             string              `json:"time"`
	Subject          string              `json:"subject"`
	StreamSequence   uint64              `json:"streamSequence"`
	ConsumerSequence uint64              `json:"consumerSequence"`
	Fields           map[string]any      `json:"fields"`
	Headers          map[string][]string `json:"headers"`
	Raw              string              `json:"raw"`
}

type QueryResponse struct {
	Source     string        `json:"source"`
	Stream     string        `json:"stream"`
	Count      int           `json:"count"`
	Scanned    int           `json:"scanned"`
	Truncated  bool          `json:"truncated"`
	DurationMS int64         `json:"durationMs"`
	Fields     []string      `json:"fields"`
	Messages   []ViewMessage `json:"messages"`
	Note       string        `json:"note,omitempty"`
}

type APIError struct {
	Error string `json:"error"`
}

func main() {
	addr := strings.TrimSpace(os.Getenv("LISTEN_ADDR"))
	if addr == "" {
		addr = "127.0.0.1:8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.HandleFunc("/api/test", handleTest)
	mux.HandleFunc("/api/query", handleQuery)
	mux.HandleFunc("/api/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "Используйте GET")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	server := &http.Server{
		Addr:              addr,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	log.Printf("Message Viewer слушает http://%s", addr)
	log.Printf("Для доступа из локальной сети задайте LISTEN_ADDR=0.0.0.0:8080; используйте только в доверенной сети или за аутентифицирующим reverse proxy")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Используйте GET")
		return
	}
	page, err := webFiles.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, "Не удалось загрузить интерфейс", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(page)
}

func handleTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Используйте POST")
		return
	}
	cfg, err := decodeRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := checkSource(cfg.Source); err != nil {
		writeError(w, http.StatusNotImplemented, err.Error())
		return
	}
	if strings.TrimSpace(cfg.Stream) == "" {
		writeError(w, http.StatusBadRequest, "Укажите имя JetStream stream")
		return
	}

	nc, err := connectNATS(cfg)
	if err != nil {
		writeError(w, http.StatusBadGateway, "Ошибка подключения к NATS: "+err.Error())
		return
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		writeError(w, http.StatusBadGateway, "Не удалось создать JetStream context: "+err.Error())
		return
	}
	info, err := js.StreamInfo(strings.TrimSpace(cfg.Stream))
	if err != nil {
		writeError(w, http.StatusBadGateway, "Не удалось получить stream info: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"stream":        info.Config.Name,
		"messages":      info.State.Msgs,
		"bytes":         info.State.Bytes,
		"firstSequence": info.State.FirstSeq,
		"lastSequence":  info.State.LastSeq,
		"firstTime":     info.State.FirstTime,
		"lastTime":      info.State.LastTime,
		"subjects":      info.Config.Subjects,
	})
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Используйте POST")
		return
	}
	cfg, err := decodeRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := checkSource(cfg.Source); err != nil {
		writeError(w, http.StatusNotImplemented, err.Error())
		return
	}

	start, end, err := validateRequest(&cfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	startedAt := time.Now()
	ctxDeadline := time.Now().Add(time.Duration(cfg.TimeoutSeconds) * time.Second)
	nc, err := connectNATS(cfg)
	if err != nil {
		writeError(w, http.StatusBadGateway, "Ошибка подключения к NATS: "+err.Error())
		return
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		writeError(w, http.StatusBadGateway, "JetStream недоступен: "+err.Error())
		return
	}
	streamName := strings.TrimSpace(cfg.Stream)
	if _, err := js.StreamInfo(streamName); err != nil {
		writeError(w, http.StatusBadGateway, "Не удалось открыть stream "+streamName+": "+err.Error())
		return
	}

	subject := strings.TrimSpace(cfg.Subject)
	if subject == "" {
		subject = ">"
	}
	// The ephemeral JetStream consumer starts at the requested time. Unsubscribe
	// removes this temporary consumer; the viewer never creates a durable consumer.
	sub, err := js.SubscribeSync(subject,
		nats.BindStream(streamName),
		nats.StartTime(start),
		nats.AckNone(),
		nats.MaxDeliver(1),
		nats.InactiveThreshold(2*time.Minute),
	)
	if err != nil {
		writeError(w, http.StatusBadGateway, "Не удалось создать временную подписку JetStream: "+err.Error())
		return
	}
	defer func() { _ = sub.Unsubscribe() }()

	resp := QueryResponse{
		Source:   "nats",
		Stream:   streamName,
		Messages: make([]ViewMessage, 0, min(cfg.Limit, 512)),
	}
	readTimeout := 1500 * time.Millisecond
	if timeout := time.Duration(cfg.TimeoutSeconds) * time.Second; timeout < readTimeout {
		readTimeout = timeout
	}
	if readTimeout < 100*time.Millisecond {
		readTimeout = 100 * time.Millisecond
	}
	fieldSet := map[string]struct{}{}
	finishedByEnd := false

	for resp.Scanned < cfg.MaxScan && time.Now().Before(ctxDeadline) {
		msg, nextErr := sub.NextMsg(readTimeout)
		if nextErr != nil {
			if errors.Is(nextErr, nats.ErrTimeout) || strings.Contains(strings.ToLower(nextErr.Error()), "timeout") {
				// Historical messages are delivered as a finite backlog. An idle read
				// means there is currently nothing else to read in the selected range.
				break
			}
			if errors.Is(nextErr, nats.ErrConnectionClosed) || errors.Is(nextErr, nats.ErrDisconnected) {
				writeError(w, http.StatusBadGateway, "Соединение с NATS разорвано: "+nextErr.Error())
				return
			}
			writeError(w, http.StatusBadGateway, "Ошибка чтения JetStream: "+nextErr.Error())
			return
		}
		meta, metaErr := msg.Metadata()
		if metaErr != nil {
			continue
		}
		// A message after the selected end tells us the consumer passed the range.
		if meta.Timestamp.After(end) {
			finishedByEnd = true
			break
		}
		if meta.Timestamp.Before(start) {
			continue
		}
		resp.Scanned++

		view := toViewMessage(msg, meta.Timestamp, meta.Sequence.Stream, meta.Sequence.Consumer)
		for key := range view.Fields {
			fieldSet[key] = struct{}{}
		}
		resp.Messages = append(resp.Messages, view)
		// Keep the newest N records inside the selected period rather than the oldest N.
		if len(resp.Messages) > cfg.Limit {
			copy(resp.Messages, resp.Messages[len(resp.Messages)-cfg.Limit:])
			resp.Messages = resp.Messages[:cfg.Limit]
		}
	}

	resp.Count = len(resp.Messages)
	resp.DurationMS = time.Since(startedAt).Milliseconds()
	// Only expose columns used by the rows that are actually shown in the UI.
	fieldSet = map[string]struct{}{}
	for _, visible := range resp.Messages {
		for f := range visible.Fields {
			fieldSet[f] = struct{}{}
		}
	}
	for f := range fieldSet {
		resp.Fields = append(resp.Fields, f)
	}
	sort.Strings(resp.Fields)
	if resp.Scanned >= cfg.MaxScan && !finishedByEnd {
		resp.Truncated = true
		resp.Note = fmt.Sprintf("Достигнут предел просмотра %d сообщений. Увеличьте «Макс. сообщений просмотреть» или сузьте период.", cfg.MaxScan)
	} else if time.Now().After(ctxDeadline) {
		resp.Truncated = true
		resp.Note = "Истёк лимит времени запроса; показана часть результатов. Увеличьте тайм-аут или сузьте период."
	} else if resp.Count == 0 {
		resp.Note = "Сообщений в выбранном диапазоне не найдено. Проверьте stream, subject и период; история должна храниться в JetStream."
	} else {
		resp.Note = "Показаны последние сообщения из выбранного диапазона времени. «Просмотрено» — сколько сообщений было прочитано из JetStream для поиска последних записей."
	}
	writeJSON(w, http.StatusOK, resp)
}

func decodeRequest(w http.ResponseWriter, r *http.Request) (QueryRequest, error) {
	var cfg QueryRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return cfg, fmt.Errorf("пустое тело запроса")
		}
		return cfg, fmt.Errorf("неверный JSON: %w", err)
	}
	return cfg, nil
}

func checkSource(source string) error {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "nats", "":
		return nil
	case "kafka":
		return fmt.Errorf("Kafka пока является UI-заготовкой: адаптер чтения ещё не реализован")
	case "rabbitmq":
		return fmt.Errorf("RabbitMQ пока является UI-заготовкой: адаптер чтения ещё не реализован")
	default:
		return fmt.Errorf("неизвестный источник %q", source)
	}
}

func validateRequest(cfg *QueryRequest) (time.Time, time.Time, error) {
	if strings.TrimSpace(cfg.Stream) == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("укажите имя JetStream stream")
	}
	if strings.TrimSpace(cfg.NATSURL) == "" {
		cfg.NATSURL = defaultNATSURL
	}
	start, err := parseTime(cfg.Start)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("неверное значение «С»: %w", err)
	}
	end, err := parseTime(cfg.End)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("неверное значение «По»: %w", err)
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("время «По» должно быть позже времени «С»")
	}
	if end.Sub(start) > 366*24*time.Hour {
		return time.Time{}, time.Time{}, fmt.Errorf("за один запрос разрешён период не больше 366 дней")
	}
	if cfg.Limit < 1 {
		cfg.Limit = 1
	}
	if cfg.Limit > 5000 {
		cfg.Limit = 5000
	}
	if cfg.TimeoutSeconds < 1 {
		cfg.TimeoutSeconds = 30
	}
	if cfg.TimeoutSeconds > 300 {
		cfg.TimeoutSeconds = 300
	}
	if cfg.MaxScan < 1 {
		cfg.MaxScan = defaultMaxScan
	}
	if cfg.MaxScan > absoluteMaxScan {
		cfg.MaxScan = absoluteMaxScan
	}
	return start, end, nil
}

func parseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("поле не заполнено")
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("ожидалась дата со смещением часового пояса (RFC3339)")
}

func connectNATS(cfg QueryRequest) (*nats.Conn, error) {
	url := strings.TrimSpace(cfg.NATSURL)
	if url == "" {
		url = defaultNATSURL
	}
	opts := []nats.Option{
		nats.Name("message-viewer"),
		nats.Timeout(5 * time.Second),
		nats.MaxReconnects(0),
	}
	if strings.TrimSpace(cfg.Token) != "" {
		opts = append(opts, nats.Token(cfg.Token))
	} else if strings.TrimSpace(cfg.User) != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	return nats.Connect(url, opts...)
}

func toViewMessage(msg *nats.Msg, timestamp time.Time, streamSeq, consumerSeq uint64) ViewMessage {
	fields := map[string]any{}
	var value any
	if len(msg.Data) <= maxMessageBytes && json.Unmarshal(msg.Data, &value) == nil {
		flattenValue(fields, "", value, 0)
	} else if len(msg.Data) > maxMessageBytes {
		fields["body"] = fmt.Sprintf("[payload больше %d байт; полный текст не разбирался]", maxMessageBytes)
	} else {
		fields["body_text"] = string(msg.Data)
	}

	headers := map[string][]string{}
	for key, values := range msg.Header {
		copyValues := append([]string(nil), values...)
		headers[key] = copyValues
		fields["header."+key] = strings.Join(copyValues, ", ")
	}
	raw := string(msg.Data)
	if len(msg.Data) <= maxMessageBytes {
		var pretty any
		if json.Unmarshal(msg.Data, &pretty) == nil {
			if data, err := json.MarshalIndent(pretty, "", "  "); err == nil {
				raw = string(data)
			}
		}
	} else {
		raw = fmt.Sprintf("[payload %d байт; вывод полного payload отключён на стороне сервера]", len(msg.Data))
	}
	return ViewMessage{
		Time:             timestamp.UTC().Format(time.RFC3339Nano),
		Subject:          msg.Subject,
		StreamSequence:   streamSeq,
		ConsumerSequence: consumerSeq,
		Fields:           fields,
		Headers:          headers,
		Raw:              raw,
	}
}

func flattenValue(out map[string]any, prefix string, value any, depth int) {
	if depth > 8 {
		if prefix != "" {
			out[prefix] = "[максимальная глубина вложенности]"
		}
		return
	}
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 && prefix != "" {
			out[prefix] = "{}"
			return
		}
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			flattenValue(out, path, v[key], depth+1)
		}
	case []any:
		if prefix != "" {
			if b, err := json.Marshal(v); err == nil {
				out[prefix] = string(b)
			}
		}
	default:
		if prefix == "" {
			out["value"] = v
		} else {
			out[prefix] = v
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, APIError{Error: message})
}
