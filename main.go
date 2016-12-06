package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"
)

const (
	BadRequestAsset    = "400.html"
	NotFoundAsset      = "404.html"
	InternalErrorAsset = "500.html"
	HomepageAsset      = "index.html"
	ExpiredAsset       = "expired.html"
	NotYetAsset        = "not_yet.html"
)

const (
	IDBase     = 2
	MaxURLSize = 8192
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "Usage: urlengthener db_file asset_dir port")
		os.Exit(1)
	}
	dbFile := os.Args[1]
	assetDir := os.Args[2]
	port := os.Args[3]

	store, err := NewKVStore(dbFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to load or create DB:", err)
		os.Exit(1)
	}

	handler := &Handler{Store: store, Assets: assetDir}
	http.HandleFunc("/asset/", handler.ServeAsset)
	http.HandleFunc("/lengthened/", handler.ServeLengthened)
	http.HandleFunc("/lengthen", handler.ServeLengthen)
	http.HandleFunc("/", handler.ServeRoot)

	// TODO: catch kill signal and properly close DB.

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to listen:", err)
		os.Exit(1)
	}
}

type DBEntry struct {
	URL   string
	Start time.Time
	End   time.Time
}

type Handler struct {
	Store  *KVStore
	Assets string
}

func (h *Handler) ServeAsset(w http.ResponseWriter, r *http.Request) {
	prefix := "/asset"
	h.serveNamedAsset(w, r, r.URL.Path[len(prefix):])
}

func (h *Handler) ServeLengthened(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(path.Base(r.URL.Path), IDBase, 64)
	if err != nil {
		h.serveNamedAsset(w, r, NotFoundAsset)
		return
	}

	entryData, err := h.Store.Get(id)
	if err != nil {
		h.serveNamedAsset(w, r, InternalErrorAsset)
		return
	} else if entryData == nil {
		h.serveNamedAsset(w, r, NotFoundAsset)
		return
	}

	var entry DBEntry
	json.Unmarshal(entryData, &entry)

	now := time.Now()
	if !entry.End.IsZero() && now.After(entry.End) {
		h.serveNamedAsset(w, r, ExpiredAsset)
		return
	} else if !entry.Start.IsZero() && !now.After(entry.Start) {
		h.serveNamedAsset(w, r, NotYetAsset)
		return
	}

	http.Redirect(w, r, entry.URL, http.StatusTemporaryRedirect)
}

func (h *Handler) ServeLengthen(w http.ResponseWriter, r *http.Request) {
	shortenURL := r.FormValue("url")
	delay := r.FormValue("delay")
	duration := r.FormValue("duration")

	if len(shortenURL) > MaxURLSize {
		h.serveNamedAsset(w, r, BadRequestAsset)
		return
	}

	entry := DBEntry{URL: shortenURL}
	if delay != "" {
		delaySecs, _ := strconv.Atoi(delay)
		entry.Start = time.Now().Add(time.Second * time.Duration(delaySecs))
	}
	if duration != "" {
		start := time.Now()
		if delay != "" {
			start = entry.Start
		}
		durSecs, _ := strconv.Atoi(duration)
		entry.End = start.Add(time.Second * time.Duration(durSecs))
	}

	data, _ := json.Marshal(&entry)
	id, err := h.Store.Insert(data)

	if err != nil {
		h.serveNamedAsset(w, r, InternalErrorAsset)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(strconv.FormatInt(id, IDBase)))
}

func (h *Handler) ServeRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		h.serveNamedAsset(w, r, NotFoundAsset)
		return
	}
	h.serveNamedAsset(w, r, HomepageAsset)
}

func (h *Handler) serveNamedAsset(w http.ResponseWriter, r *http.Request, name string) {
	dir := http.Dir(h.Assets)
	f, err := dir.Open(name)
	if err != nil {
		if name != NotFoundAsset {
			h.serveNamedAsset(w, r, NotFoundAsset)
		} else {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("failed to load 404 page"))
		}
		return
	}
	defer f.Close()

	statusCodes := map[string]int{
		NotFoundAsset:      404,
		InternalErrorAsset: 500,
		BadRequestAsset:    400,
	}
	if code, ok := statusCodes[name]; ok {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(code)
		io.Copy(w, f)
		return
	} else {
		http.ServeContent(w, r, name, time.Now(), f)
	}
}
