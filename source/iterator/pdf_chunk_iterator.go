package iterator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/dslipak/pdf"
)

// ChunkingStrategy defines how PDFs should be chunked
type ChunkingStrategy string

const (
	ChunkByPage      ChunkingStrategy = "page"
	ChunkByParagraph ChunkingStrategy = "paragraph"
	ChunkBySize      ChunkingStrategy = "size"
)

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (opencdc.Record, error)
	Stop()
}

// PdfChunk represents a single chunk from a PDF document
type PdfChunk struct {
	Content    string            `json:"content"`
	PageNumber int               `json:"pageNumber"`
	ChunkIndex int               `json:"chunkIndex"`
	Metadata   map[string]string `json:"metadata"`
}

// PdfChunkingConfig holds configuration for PDF chunking
type PdfChunkingConfig struct {
	// Defines how to chunk the PDF
	Strategy ChunkingStrategy
	// Maximum size of each chunk in characters (used if Strategy is ChunkBySize)
	ChunkSize int
	// How much overlap to include between chunks (percentage)
	ChunkOverlap int
}

// PdfChunkIterator wraps another iterator and adds PDF chunking capability
type PdfChunkIterator struct {
	baseIterator Iterator
	config       PdfChunkingConfig

	// Current file being processed
	currentKey    string
	currentFile   []byte
	currentChunks []PdfChunk
	chunkIndex    int

	// For position tracking
	currentPosition position.Position
	startPosition   position.Position
	resumeFromChunk bool
}

// NewPdfChunkIterator creates a new iterator that chunks PDFs
func NewPdfChunkIterator(baseIterator Iterator, config PdfChunkingConfig, startPosition position.Position) *PdfChunkIterator {
	return &PdfChunkIterator{
		baseIterator:    baseIterator,
		config:          config,
		chunkIndex:      -1, // Start with -1 so first increment gives 0
		startPosition:   startPosition,
		resumeFromChunk: startPosition.IsChunkPosition(), // Flag to indicate we need to resume
	}
}

// HasNext returns true if there are more chunks or files to process
func (p *PdfChunkIterator) HasNext(ctx context.Context) bool {

	// If we have chunks in the buffer, return true
	if p.chunkIndex < len(p.currentChunks)-1 {
		return true
	}

	// Otherwise check if the base iterator has more files
	return p.baseIterator.HasNext(ctx)
}

// Next returns the next record (either a chunk from the current file or from a new file)
func (p *PdfChunkIterator) Next(ctx context.Context) (opencdc.Record, error) {
	// If we have chunks in the buffer, return the next one
	if p.chunkIndex < len(p.currentChunks)-1 {
		p.chunkIndex++
		return p.createRecordFromChunk(ctx, p.currentChunks[p.chunkIndex])
	}

	// Get the next file from the base iterator
	record, err := p.baseIterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, err
	}

	// Store the original record's position for later use
	rp, err := position.ParseRecordPosition(record.Position)

	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to parse position: %w", err)
	}

	p.currentPosition = rp

	// Check if the file is a PDF
	contentType, ok := record.Metadata[MetadataS3HeaderPrefix+MetadataContentType]
	if !ok || contentType != "application/pdf" {
		// If not a PDF, just return the record as-is
		p.resumeFromChunk = false // Reset flag
		return record, nil
	}

	// Store the current file info
	p.currentKey = string(record.Key.Bytes())
	p.currentFile = record.Payload.After.Bytes()

	// Process the PDF and generate chunks
	chunks, err := p.processPdf(ctx, p.currentFile, p.currentKey, record.Metadata)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to process PDF: %w", err)
	}

	// Store chunks
	p.currentChunks = chunks

	// Check if we need to resume from a specific chunk
	if p.resumeFromChunk && p.currentKey == p.startPosition.Key {
		// We found the file we need to resume from
		if p.startPosition.ChunkIndex < len(chunks) {
			// Set the index to start from
			p.chunkIndex = p.startPosition.ChunkIndex - 1 // -1 because Next will increment it
			p.resumeFromChunk = false                     // Reset flag
		} else {
			// If the requested chunk index is out of bounds, start from the beginning
			p.chunkIndex = -1
			p.resumeFromChunk = false // Reset flag
		}
	} else {
		// Start from the beginning of this file
		p.chunkIndex = -1
		p.resumeFromChunk = false // Reset flag if this isn't the file we're looking for
	}

	// Call Next recursively to get the next chunk
	return p.Next(ctx)
}

// Stop forwards the call to the base iterator
func (p *PdfChunkIterator) Stop() {
	p.baseIterator.Stop()
}

// createRecordFromChunk creates a new record from a PDF chunk
func (p *PdfChunkIterator) createRecordFromChunk(ctx context.Context, chunk PdfChunk) (opencdc.Record, error) {
	// Create a new position for this chunk that combines the original position with chunk info
	chunkPos := position.Position{
		Key:         p.currentPosition.Key,
		Type:        p.currentPosition.Type,
		Timestamp:   p.currentPosition.Timestamp,
		ChunkIndex:  chunk.ChunkIndex,
		TotalChunks: len(p.currentChunks),
	}

	posBytes, err := json.Marshal(chunkPos)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to marshal position: %w", err)
	}

	// Create metadata with chunk info
	metadata := opencdc.Metadata{}

	for k, v := range chunk.Metadata {
		metadata[k] = v
	}
	metadata["pdf.pageNumber"] = fmt.Sprintf("%d", chunk.PageNumber)
	metadata["pdf.chunkIndex"] = fmt.Sprintf("%d", chunk.ChunkIndex)
	metadata["pdf.totalChunks"] = fmt.Sprintf("%d", len(p.currentChunks))
	metadata["pdf.originalKey"] = p.currentKey

	// Generate record ID that includes the chunk information
	recordKey := fmt.Sprintf("%s-chunk-%d", p.currentKey, chunk.ChunkIndex)

	// Create a payload with the chunk content
	payloadData, err := json.Marshal(chunk)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to marshal chunk: %w", err)
	}

	// Create the record with the appropriate operation type (same as original record)
	var record opencdc.Record
	switch p.currentPosition.Type {
	case position.TypeCDC:
		// For CDC, use Create operation (we don't have operation type in position)
		record = sdk.Util.Source.NewRecordCreate(
			posBytes,
			metadata,
			opencdc.RawData(recordKey),
			opencdc.RawData(payloadData),
		)
	default:
		// For snapshot, use snapshot operation
		record = sdk.Util.Source.NewRecordSnapshot(
			posBytes,
			metadata,
			opencdc.RawData(recordKey),
			opencdc.RawData(payloadData),
		)
	}

	return record, nil
}

// processPdf processes a PDF file and returns chunks based on the configured strategy
func (p *PdfChunkIterator) processPdf(ctx context.Context, pdfData []byte, key string, metadata opencdc.Metadata) ([]PdfChunk, error) {
	// Create a reader from the PDF data
	r := bytes.NewReader(pdfData)
	pdfr, err := pdf.NewReader(r, r.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to extract text from PDF: %w", err)
	}

	extractedText, err := p.extractTextFromPdf(pdfr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract text from PDF: %w", err)
	}

	// Apply chunking strategy
	var chunks []PdfChunk
	switch p.config.Strategy {
	case ChunkByPage:
		chunks = p.chunkByPage(extractedText, key, metadata)
	case ChunkByParagraph:
		chunks = p.chunkByParagraph(extractedText, key, metadata)
	case ChunkBySize:
		chunks = p.chunkBySize(extractedText, key, metadata, p.config.ChunkSize, p.config.ChunkOverlap)
	default:
		// Default to page-based chunking
		chunks = p.chunkByPage(extractedText, key, metadata)
	}

	return chunks, nil
}

func (p *PdfChunkIterator) extractTextFromPdf(r *pdf.Reader) (map[int]string, error) {
	totalPages := r.NumPage()

	result := make(map[int]string)
	for pageIndex := 1; pageIndex <= totalPages; pageIndex++ {
		text, err := r.Page(pageIndex).GetPlainText(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get pdf text: %w", err)
		}
		result[pageIndex] = text
	}

	return result, nil
}

// chunkByPage creates one chunk per page
func (p *PdfChunkIterator) chunkByPage(pageTexts map[int]string, key string, metadata opencdc.Metadata) []PdfChunk {
	chunks := make([]PdfChunk, 0, len(pageTexts))

	// Create metadata map from the original metadata
	chunkMetadata := make(map[string]string)
	for k, v := range metadata {
		chunkMetadata[k] = v
	}
	chunkMetadata["source"] = key
	chunkMetadata["strategy"] = string(p.config.Strategy)

	// Create one chunk per page, sorted by page number
	chunkIndex := 0
	for pageNum := 1; pageNum <= len(pageTexts); pageNum++ {
		if text, ok := pageTexts[pageNum]; ok {
			chunks = append(chunks, PdfChunk{
				Content:    text,
				PageNumber: pageNum,
				ChunkIndex: chunkIndex,
				Metadata:   copyMetadata(chunkMetadata),
			})
			chunkIndex++
		}
	}

	return chunks
}

// chunkByParagraph splits PDF text into paragraphs
func (p *PdfChunkIterator) chunkByParagraph(pageTexts map[int]string, key string, metadata opencdc.Metadata) []PdfChunk {
	chunks := make([]PdfChunk, 0)

	// Create metadata map from the original metadata
	chunkMetadata := make(map[string]string)
	for k, v := range metadata {
		chunkMetadata[k] = v
	}
	chunkMetadata["source"] = key
	chunkMetadata["strategy"] = string(p.config.Strategy)

	// Process each page
	chunkIndex := 0
	for pageNum := 1; pageNum <= len(pageTexts); pageNum++ {
		text, ok := pageTexts[pageNum]
		if !ok {
			continue
		}

		// Split text into paragraphs (double newline as separator)
		paragraphs := strings.Split(text, "\n\n")

		// Create a chunk for each non-empty paragraph
		for _, para := range paragraphs {
			para = strings.TrimSpace(para)
			if para == "" {
				continue
			}

			chunks = append(chunks, PdfChunk{
				Content:    para,
				PageNumber: pageNum,
				ChunkIndex: chunkIndex,
				Metadata:   copyMetadata(chunkMetadata),
			})
			chunkIndex++
		}
	}

	return chunks
}

// chunkBySize splits text into chunks of specified size with overlap
func (p *PdfChunkIterator) chunkBySize(pageTexts map[int]string, key string, metadata opencdc.Metadata, chunkSize int, overlapPercent int) []PdfChunk {
	chunks := make([]PdfChunk, 0)

	// Create metadata map from the original metadata
	chunkMetadata := make(map[string]string)
	for k, v := range metadata {
		chunkMetadata[k] = v
	}
	chunkMetadata["source"] = key
	chunkMetadata["strategy"] = string(p.config.Strategy)

	// Calculate overlap size
	overlapSize := (chunkSize * overlapPercent) / 100

	// Process each page
	chunkIndex := 0
	for pageNum := 1; pageNum <= len(pageTexts); pageNum++ {
		text, ok := pageTexts[pageNum]
		if !ok {
			continue
		}

		// Skip empty pages
		if strings.TrimSpace(text) == "" {
			continue
		}

		// If the text is smaller than chunk size, use as a single chunk
		if len(text) <= chunkSize {
			chunks = append(chunks, PdfChunk{
				Content:    text,
				PageNumber: pageNum,
				ChunkIndex: chunkIndex,
				Metadata:   copyMetadata(chunkMetadata),
			})
			chunkIndex++
			continue
		}

		// Split text into chunks with overlap
		offset := 0
		for offset < len(text) {
			// Determine end of this chunk
			end := offset + chunkSize
			if end > len(text) {
				end = len(text)
			}

			// Extract chunk text
			chunkText := text[offset:end]

			// Try to end at a sentence boundary if possible
			if end < len(text) {
				// Look for a sentence boundary within the last quarter of the chunk
				searchStart := end - (chunkSize / 4)
				if searchStart < offset {
					searchStart = offset
				}

				// Find last period, question mark or exclamation mark followed by space or newline
				lastSentence := -1
				for i := end - 1; i >= searchStart; i-- {
					if i+1 < len(text) && (text[i] == '.' || text[i] == '?' || text[i] == '!') &&
						(text[i+1] == ' ' || text[i+1] == '\n') {
						lastSentence = i + 1
						break
					}
				}

				if lastSentence > 0 {
					end = lastSentence
					chunkText = text[offset:end]
				}
			}

			// Create chunk
			chunks = append(chunks, PdfChunk{
				Content:    chunkText,
				PageNumber: pageNum,
				ChunkIndex: chunkIndex,
				Metadata:   copyMetadata(chunkMetadata),
			})
			chunkIndex++

			// Move offset for next chunk, accounting for overlap
			offset = end - overlapSize
			if offset <= 0 || offset >= len(text) {
				break
			}
		}
	}

	return chunks
}

// copyMetadata creates a copy of a metadata map
func copyMetadata(m map[string]string) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
