package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

func (s *httpServer) handleAppend(w http.ResponseWriter, r *http.Request) {
	var request AppendRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := s.Log.Append(request.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := AppendResponse{
		Offset: offset,
	}
	if err := json.NewEncoder(w).Encode(&response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *httpServer) handleRetrieve(w http.ResponseWriter, r *http.Request) {
	var request RetrieveRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(request.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := RetrieveResponse{Record: record}
	if err := json.NewEncoder(w).Encode(&response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func NewHttpServer(address string) *http.Server {
	server := &httpServer{
		Log: NewLog(),
	}

	router := mux.NewRouter()
	router.HandleFunc("/", server.handleAppend).Methods("POST")
	router.HandleFunc("/", server.handleRetrieve).Methods("GET")

	return &http.Server{
		Addr:    address,
		Handler: router,
	}
}

type AppendRequest struct {
	Record Record `json:"record"`
}

type AppendResponse struct {
	Offset uint64 `json:"offset"`
}

type RetrieveRequest struct {
	Offset uint64 `json:"offset"`
}

type RetrieveResponse struct {
	Record Record `json:"record"`
}
