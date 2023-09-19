// Copyright 2023 The Prometheus Authors
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

package rules

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/prometheus/prometheus/model/labels"
)

/////////////////////////// StaleSeriesRepository ///////////////////////////

type StaleSeriesRepository interface {
	Clear()
	InitCurrent(length int)
	AddToCurrent(ls labels.Labels)
	ForEachStale(ruleId int, fn func(lset labels.Labels))
	MoveCurrentToPrevious(ruleId int) error
	GetNumberOfRules() int
	GetAll(ruleId int) []labels.Labels
	CopyFrom(ruleId int, other StaleSeriesRepository, otherRuleId int) error
}

var NewStaleSeriesRepositoryForTesting = NewStaleSeriesDiskRepository

/////////////////////////// StaleSeriesMemoryRepository ///////////////////////////

type StaleSeriesMemoryRepository struct {
	seriesReturned       map[string]bool
	seriesInPreviousEval []map[string]bool
	numberOfRules        int
}

func NewStaleSeriesMemoryRepository(numberOfRules int) *StaleSeriesMemoryRepository {
	return &StaleSeriesMemoryRepository{
		seriesInPreviousEval: make([]map[string]bool, numberOfRules),
		numberOfRules:        numberOfRules,
	}
}

func (r *StaleSeriesMemoryRepository) InitCurrent(length int) {
	r.seriesReturned = make(map[string]bool, length)
}

func (r *StaleSeriesMemoryRepository) AddToCurrent(ls labels.Labels) {
	buf := [1024]byte{}
	r.seriesReturned[string(ls.Bytes(buf[:]))] = true
}

func (r *StaleSeriesMemoryRepository) Clear() {
	r.seriesInPreviousEval = nil
	r.seriesReturned = nil
}

func (r *StaleSeriesMemoryRepository) ForEachStale(ruleId int, fn func(lset labels.Labels)) {
	for metric, _ := range r.seriesInPreviousEval[ruleId] {
		if _, ok := r.seriesReturned[metric]; !ok {
			lset, err := labels.FromBytes([]byte(metric))
			if err != nil {
				panic(err) //TOOD!
			}
			fn(*lset)
		}
	}
}

func (r *StaleSeriesMemoryRepository) GetAll(ruleId int) []labels.Labels {
	ls := make([]labels.Labels, 0, len(r.seriesInPreviousEval[ruleId]))
	for metric, _ := range r.seriesInPreviousEval[ruleId] {
		lset, err := labels.FromBytes([]byte(metric))
		if err != nil {
			panic(err) //TOOD!
		}
		ls = append(ls, *lset)
	}
	return ls
}

func (r *StaleSeriesMemoryRepository) MoveCurrentToPrevious(ruleId int) error {
	r.seriesInPreviousEval[ruleId] = r.seriesReturned
	r.seriesReturned = nil
	return nil
}

func (r *StaleSeriesMemoryRepository) KeyCount(ruleId int) int {
	if r.seriesInPreviousEval[ruleId] == nil {
		return 0
	}
	return len(r.seriesInPreviousEval[ruleId])
}

func (r *StaleSeriesMemoryRepository) CopyFrom(ruleId int, other StaleSeriesRepository, otherRuleId int) error {
	otherAsMemoryRepo := other.(*StaleSeriesMemoryRepository)
	if otherAsMemoryRepo.seriesInPreviousEval[otherRuleId] == nil {
		return nil //TODO?
	}

	r.seriesInPreviousEval[ruleId] = otherAsMemoryRepo.seriesInPreviousEval[otherRuleId]
	return nil
}

func (r *StaleSeriesMemoryRepository) GetNumberOfRules() int {
	return r.numberOfRules
}

//////////////////////// StaleSeriesDiskRepository	///////////////////////////

var seps = []byte{'\r', '\n'}

type StaleSeriesDiskRepository struct {
	seriesReturned map[string]bool
	baseDir        string
	files          []string
	numberOfRules  int
	keyCount       []int
}

func NewStaleSeriesDiskRepository(numberOfRules int) *StaleSeriesDiskRepository {
	dname, _ := os.MkdirTemp("", "stale_series") //todo error
	return &StaleSeriesDiskRepository{
		baseDir:       dname,
		files:         make([]string, numberOfRules),
		numberOfRules: numberOfRules,
		keyCount:      make([]int, numberOfRules),
	}
}

func (r *StaleSeriesDiskRepository) InitCurrent(length int) {
	r.seriesReturned = make(map[string]bool, length)
}

func (r *StaleSeriesDiskRepository) AddToCurrent(ls labels.Labels) {
	buf := [1024]byte{}
	r.seriesReturned[string(ls.Bytes(buf[:]))] = true
}

func (r *StaleSeriesDiskRepository) Clear() {
	os.RemoveAll(r.baseDir)
}

func (r *StaleSeriesDiskRepository) ForEachStale(ruleId int, fn func(lset labels.Labels)) {
	r.forEach(ruleId, func(key string) {
		if _, ok := r.seriesReturned[key]; !ok {
			lset, err := labels.FromBytes([]byte(key))
			if err != nil {
				panic(err) //TOOD!
			}
			fn(*lset)
		}
	})
}

func (r *StaleSeriesDiskRepository) MoveCurrentToPrevious(ruleId int) error {
	if r.seriesReturned == nil || len(r.seriesReturned) == 0 {
		//clear file
		r.files[ruleId] = ""
		r.keyCount[ruleId] = 0
		return nil
	}

	fpath := filepath.Join(r.baseDir, fmt.Sprintf("rule%d", ruleId))
	file, err := os.Create(fpath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	r.files[ruleId] = fpath
	sizeBuf := make([]byte, 4)
	for k, _ := range r.seriesReturned {
		buf := []byte(k)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(buf)))
		_, err = file.Write(sizeBuf)
		if err != nil {
			panic(err) //TODO: remove this
		}
		_, err = file.Write(buf)
		if err != nil {
			panic(err) //TODO: remove this
		}
	}
	r.keyCount[ruleId] = len(r.seriesReturned)
	r.seriesReturned = nil
	return nil
}

func (r *StaleSeriesDiskRepository) GetAll(ruleId int) []labels.Labels {
	ls := make([]labels.Labels, 0, r.KeyCount(ruleId))
	r.forEach(ruleId, func(key string) {
		lset, err := labels.FromBytes([]byte(key))
		if err != nil {
			panic(err) //TOOD!
		}
		ls = append(ls, *lset)
	})
	return ls
}

func (r *StaleSeriesDiskRepository) KeyCount(ruleId int) int {
	if r.files[ruleId] == "" {
		return 0
	}
	return r.keyCount[ruleId]
}

func (r *StaleSeriesDiskRepository) CopyFrom(ruleId int, other StaleSeriesRepository, otherRuleId int) error {
	otherAsDiskRepo := other.(*StaleSeriesDiskRepository)
	if otherAsDiskRepo.files[otherRuleId] == "" {
		return nil //TODO?
	}

	fpath := filepath.Join(r.baseDir, fmt.Sprintf("rule%d", ruleId))

	_, err := copy(otherAsDiskRepo.files[otherRuleId], fpath)
	if err != nil {
		panic(err)
	}

	r.files[ruleId] = fpath

	r.keyCount[ruleId] = otherAsDiskRepo.keyCount[otherRuleId]
	return nil
}

func (r *StaleSeriesDiskRepository) GetNumberOfRules() int {
	return r.numberOfRules
}

/////////////////// private methods

func (r *StaleSeriesDiskRepository) forEach(ruleId int, fn func(key string)) {
	if r.files[ruleId] == "" {
		return
	}

	source, err := os.Open(r.files[ruleId])
	if err != nil {
		panic(err)
	}
	defer source.Close()

	fileScanner := bufio.NewScanner(source)
	fileScanner.Split(scanBySize)

	for fileScanner.Scan() {
		key := fileScanner.Text()
		fn(key)
	}
}

func copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func scanBySize(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	size := int(binary.BigEndian.Uint32(data[:4]))
	return size + 4, data[4 : size+4], nil

}
