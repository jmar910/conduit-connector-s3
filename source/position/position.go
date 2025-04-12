// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package position

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

const (
	TypeSnapshot Type = iota
	TypeCDC
)

const (
	snapshotPrefixChar = 's'
	cdcPrefixChar      = 'c'

	pdfChunkPrefixChar = 'p'
)

type Type int

type Position struct {
	Key       string
	Timestamp time.Time
	Type      Type
	
	// PDF chunking fields
	ChunkIndex  int
	TotalChunks int
}

// IsChunkPosition returns true if this is a PDF chunk position
func (p Position) IsChunkPosition() bool {
	return p.ChunkIndex > 0 || p.TotalChunks > 0
}

func ParseRecordPosition(p opencdc.Position) (Position, error) {
	if p == nil {
		// empty Position would have the fields with their default values
		return Position{}, nil
	}
	s := string(p)

	if strings.Contains(s, "_p") {
		return parsePdfChunkPosition(s)
	}

	index := strings.LastIndex(s, "_")
	if index == -1 {
		return Position{}, errors.New("invalid position format, no '_' found")
	}
	seconds, err := strconv.ParseInt(s[index+2:], 10, 64)
	if err != nil {
		return Position{}, fmt.Errorf("could not parse the position timestamp: %w", err)
	}

	if s[index+1] != cdcPrefixChar && s[index+1] != snapshotPrefixChar {
		return Position{}, fmt.Errorf("invalid position format, no '%c' or '%c' after '_'", snapshotPrefixChar, cdcPrefixChar)
	}
	pType := TypeSnapshot
	if s[index+1] == cdcPrefixChar {
		pType = TypeCDC
	}

	return Position{
		Key:       s[:index],
		Timestamp: time.Unix(seconds, 0),
		Type:      pType,
	}, err
}

// parsePdfChunkPosition parses a position string with PDF chunk information
// Format: key_pTIMESTAMP_CHUNKINDEX_TOTALCHUNKS_MODE
func parsePdfChunkPosition(s string) (Position, error) {
	parts := strings.Split(s, "_")
	if len(parts) < 5 {
		return Position{}, fmt.Errorf("invalid PDF chunk position format, expected at least 5 parts but got %d", len(parts))
	}
	
	// Extract key (could contain underscores)
	keyParts := parts[:len(parts)-4]
	key := strings.Join(keyParts, "_")
	
	// Extract timestamp
	if !strings.HasPrefix(parts[len(parts)-4], "p") {
		return Position{}, fmt.Errorf("invalid PDF chunk position format, expected 'p' prefix")
	}
	
	timestampStr := parts[len(parts)-4][1:] // Remove 'p'
	seconds, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return Position{}, fmt.Errorf("could not parse chunk position timestamp: %w", err)
	}
	
	// Extract chunk index
	chunkIndex, err := strconv.Atoi(parts[len(parts)-3])
	if err != nil {
		return Position{}, fmt.Errorf("could not parse chunk index: %w", err)
	}
	
	// Extract total chunks
	totalChunks, err := strconv.Atoi(parts[len(parts)-2])
	if err != nil {
		return Position{}, fmt.Errorf("could not parse total chunks: %w", err)
	}

	// Extract mode
	mode := parts[len(parts)-1]
	
	// Determine the position type (snapshot or CDC)
	pType := TypeSnapshot
	if mode[0] == cdcPrefixChar {
		// For in-progress chunks, use the same type as the original file
		// We'll determine this from context
		pType = TypeCDC // Default to CDC if in-progress
	}
	
	return Position{
		Key:         key,
		Timestamp:   time.Unix(seconds, 0),
		Type:        pType,
		ChunkIndex:  chunkIndex,
		TotalChunks: totalChunks,
	}, nil
}

func (p Position) ToRecordPosition() opencdc.Position {
	char := snapshotPrefixChar
	if p.Type == TypeCDC {
		char = cdcPrefixChar
	}

	// If this is a PDF chunk position, use the chunk format
	if p.IsChunkPosition() {
		return []byte(fmt.Sprintf("%s_p%d_%d_%d_%c", 
			p.Key, 
			p.Timestamp.Unix(), 
			p.ChunkIndex, 
			p.TotalChunks, 
			char))
	}

	return []byte(fmt.Sprintf("%s_%c%d", p.Key, char, p.Timestamp.Unix()))
}

func ConvertToCDCPosition(p opencdc.Position) (opencdc.Position, error) {
	cdcPos, err := ParseRecordPosition(p)
	if err != nil {
		return opencdc.Position{}, err
	}
	cdcPos.Type = TypeCDC
	return cdcPos.ToRecordPosition(), nil
}
