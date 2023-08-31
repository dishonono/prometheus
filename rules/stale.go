package rules

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"git.mills.io/prologic/bitcask"
	"github.com/prometheus/prometheus/model/labels"
)

type StaleSeriesRepository interface {
	//GetByKey(ruleId int, key string) labels.Labels
	Clear()
	ScanKeys(ruleId int, fn func(key string, valueFunc func() labels.Labels))
	//Put(ruleId int, key string, value labels.Labels) error
	PutAll(ruleId int, values map[string]labels.Labels) error
	KeyCount(ruleId int) int
	CopyFrom(ruleId int, other StaleSeriesRepository, otherRuleId int) error
	//GetNumberOfRules() int
	GetRawValues() []map[string]labels.Labels
}

var NewStaleSeriesRepositoryForTesting = NewStaleSeriesImprovedDiskRepository

/////////////////////////// StaleSeriesMemoryRepository ///////////////////////////

type StaleSeriesMemoryRepository struct {
	seriesInPreviousEval []map[string]labels.Labels
	numberOfRules        int
}

func NewStaleSeriesMemoryRepository(numberOfRules int) *StaleSeriesMemoryRepository {
	return &StaleSeriesMemoryRepository{
		seriesInPreviousEval: make([]map[string]labels.Labels, numberOfRules),
		numberOfRules:        numberOfRules,
	}
}

func (r *StaleSeriesMemoryRepository) Clear() {
	r.seriesInPreviousEval = nil
}

func (r *StaleSeriesMemoryRepository) ScanKeys(ruleId int, fn func(key string, valueFunc func() labels.Labels)) {

	var key string
	valueFunc := func() labels.Labels {

		return r.seriesInPreviousEval[ruleId][key]
	}
	for k, _ := range r.seriesInPreviousEval[ruleId] {
		key = k
		fn(k, valueFunc)
	}
}

// func (r *StaleSeriesMemoryRepository) Put(ruleId int, key string, value labels.Labels) error {
// 	if r.seriesInPreviousEval[ruleId] == nil {
// 		r.seriesInPreviousEval[ruleId] = make(map[string]labels.Labels)
// 	}
// 	r.seriesInPreviousEval[ruleId][key] = value
// 	return nil
// }

func (r *StaleSeriesMemoryRepository) PutAll(ruleId int, values map[string]labels.Labels) error {
	r.seriesInPreviousEval[ruleId] = values
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

func (r *StaleSeriesMemoryRepository) GetRawValues() []map[string]labels.Labels {
	return r.seriesInPreviousEval
}

/////////////////////////// StaleSeriesImprovedMemoryRepository ///////////////////////////

type StaleSeriesImprovedMemoryRepository struct {
	seriesInPreviousEval []map[string]bool
	numberOfRules        int
}

func NewStaleSeriesImprovedMemoryRepository(numberOfRules int) *StaleSeriesImprovedMemoryRepository {
	return &StaleSeriesImprovedMemoryRepository{
		seriesInPreviousEval: make([]map[string]bool, numberOfRules),
		numberOfRules:        numberOfRules,
	}
}

func (r *StaleSeriesImprovedMemoryRepository) Clear() {
	r.seriesInPreviousEval = nil
}

func (r *StaleSeriesImprovedMemoryRepository) ScanKeys(ruleId int, fn func(key string, valueFunc func() labels.Labels)) {

	var key string
	valueFunc := func() labels.Labels {
		return labelsFromBytes([]byte(key))
	}
	for k, _ := range r.seriesInPreviousEval[ruleId] {
		key = k
		fn(k, valueFunc)
	}
}

func (r *StaleSeriesImprovedMemoryRepository) PutAll(ruleId int, values map[string]labels.Labels) error {
	r.seriesInPreviousEval[ruleId] = make(map[string]bool, len(values))
	for k, _ := range values {
		r.seriesInPreviousEval[ruleId][k] = true
	}
	return nil
}

func (r *StaleSeriesImprovedMemoryRepository) KeyCount(ruleId int) int {
	if r.seriesInPreviousEval[ruleId] == nil {
		return 0
	}
	return len(r.seriesInPreviousEval[ruleId])
}

func (r *StaleSeriesImprovedMemoryRepository) CopyFrom(ruleId int, other StaleSeriesRepository, otherRuleId int) error {
	otherAsMemoryRepo := other.(*StaleSeriesImprovedMemoryRepository)
	if otherAsMemoryRepo.seriesInPreviousEval[otherRuleId] == nil {
		return nil //TODO?
	}

	r.seriesInPreviousEval[ruleId] = otherAsMemoryRepo.seriesInPreviousEval[otherRuleId]
	return nil
}

func (r *StaleSeriesImprovedMemoryRepository) GetNumberOfRules() int {
	return r.numberOfRules
}

func (r *StaleSeriesImprovedMemoryRepository) GetRawValues() []map[string]labels.Labels {
	data := make([]map[string]labels.Labels, r.numberOfRules)
	for i := 0; i < r.numberOfRules; i++ {
		if r.seriesInPreviousEval[i] == nil {
			continue
		}
		data[i] = make(map[string]labels.Labels)
		r.ScanKeys(i, func(key string, valueFunc func() labels.Labels) {
			data[i][key] = valueFunc()
		})
	}
	return data
}

/////////////////////////// StaleSeriesDiskRepository ///////////////////////////

type StaleSeriesDiskRepository struct {
	baseDir       string
	files         []string
	numberOfRules int
	backup        *StaleSeriesMemoryRepository
}

func NewStaleSeriesDiskRepository(numberOfRules int) *StaleSeriesDiskRepository {
	dname, _ := os.MkdirTemp("", "stale_series") //todo error
	return &StaleSeriesDiskRepository{
		baseDir:       dname,
		files:         make([]string, numberOfRules),
		numberOfRules: numberOfRules,
		backup:        NewStaleSeriesMemoryRepository(numberOfRules),
	}
}

func (r *StaleSeriesDiskRepository) ScanKeys(ruleId int, fn func(key string, valueFunc func() labels.Labels)) {
	if r.files[ruleId] == "" {
		return
	}

	db, _ := bitcask.Open(r.files[ruleId])
	defer db.Close()
	var key []byte
	var valueFunc = func() labels.Labels {
		value, _ := db.Get(key)
		return labelsFromBytes(value)
	}
	db.Scan([]byte(""), func(k []byte) error {
		key = k
		fn(string(k), valueFunc)
		return nil
	})
}

func (r *StaleSeriesDiskRepository) PutAll(ruleId int, values map[string]labels.Labels) error {
	r.ensureHasFolder(ruleId)
	db, err := bitcask.Open(r.files[ruleId], bitcask.WithMaxKeySize(1024))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.DeleteAll()
	if err != nil {
		panic(err)
	}
	for key, value := range values {
		valueBytes := labelsToBytes(value)
		err = db.Put([]byte(key), valueBytes)
		if err != nil {
			panic(err)
		}
	}
	r.backup.PutAll(ruleId, values)
	return nil
}

func (r *StaleSeriesDiskRepository) Clear() {
	os.RemoveAll(r.baseDir)
	r.backup.Clear()
}

func (r *StaleSeriesDiskRepository) KeyCount(ruleId int) int {
	if r.files[ruleId] == "" {
		return 0
	}
	db, err := bitcask.Open(r.files[ruleId])
	if err != nil {
		panic(err)
	}
	defer db.Close()
	return db.Len()
}

func (r *StaleSeriesDiskRepository) CopyFrom(ruleId int, other StaleSeriesRepository, otherRuleId int) error {
	otherAsDiskRepo := other.(*StaleSeriesDiskRepository)
	if otherAsDiskRepo.files[otherRuleId] == "" {
		return nil //TODO?
	}
	r.ensureHasFolder(ruleId)
	err := os.RemoveAll(r.files[ruleId])
	if err != nil {
		panic(err)
	}

	db, err := bitcask.Open(otherAsDiskRepo.files[otherRuleId])
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Backup(r.files[ruleId])
	if err != nil {
		panic(err)
	}
	return err
}

func (r *StaleSeriesDiskRepository) GetNumberOfRules() int {
	return r.numberOfRules
}

func (r *StaleSeriesDiskRepository) GetRawValues() []map[string]labels.Labels {
	data := make([]map[string]labels.Labels, r.numberOfRules)
	for i := 0; i < r.numberOfRules; i++ {
		if r.files[i] == "" {
			continue
		}
		data[i] = make(map[string]labels.Labels)
		db, err := bitcask.Open(r.files[i])
		if err != nil {
			panic(err)
		}
		defer db.Close()
		err = db.Scan([]byte(""), func(key []byte) error {
			value, err := db.Get(key)
			if err != nil {
				panic(err)
			}
			data[i][string(key)] = labelsFromBytes(value)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	return data
}

// ////////////////////// StaleSeriesImprovedDiskRepository	///////////////////////////
var seps = []byte{'\n'}

type StaleSeriesImprovedDiskRepository struct {
	baseDir       string
	files         []string
	numberOfRules int
	keyCount      []int
}

func NewStaleSeriesImprovedDiskRepository(numberOfRules int) *StaleSeriesImprovedDiskRepository {
	dname, _ := os.MkdirTemp("", "stale_series") //todo error
	return &StaleSeriesImprovedDiskRepository{
		baseDir:       dname,
		files:         make([]string, numberOfRules),
		numberOfRules: numberOfRules,
		keyCount:      make([]int, numberOfRules),
	}
}

func (r *StaleSeriesImprovedDiskRepository) Clear() {
	os.RemoveAll(r.baseDir)
}

func (r *StaleSeriesImprovedDiskRepository) ScanKeys(ruleId int, fn func(key string, valueFunc func() labels.Labels)) {
	if r.files[ruleId] == "" {
		return
	}

	source, err := os.Open(r.files[ruleId])
	if err != nil {
		panic(err)
	}
	defer source.Close()

	fileScanner := bufio.NewScanner(source)
	fileScanner.Split(bufio.ScanLines)

	var key string
	valueFunc := func() labels.Labels {
		return labelsFromBytes([]byte(key))
	}

	for fileScanner.Scan() {
		key = fileScanner.Text()
		fn(key, valueFunc)
	}
}

func (r *StaleSeriesImprovedDiskRepository) PutAll(ruleId int, values map[string]labels.Labels) error {
	if values == nil || len(values) == 0 {
		return nil
	}

	fpath := filepath.Join(r.baseDir, fmt.Sprintf("rule%d", ruleId))
	file, err := os.Create(fpath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	r.files[ruleId] = fpath
	for k, _ := range values {
		_, err = file.Write([]byte(k))
		if err != nil {
			panic(err)
		}
		_, err = file.Write(seps)
		if err != nil {
			panic(err)
		}
	}
	r.keyCount[ruleId] = len(values)
	return nil
}

func (r *StaleSeriesImprovedDiskRepository) KeyCount(ruleId int) int {
	if r.files[ruleId] == "" {
		return 0
	}
	return r.keyCount[ruleId]
}

func (r *StaleSeriesImprovedDiskRepository) CopyFrom(ruleId int, other StaleSeriesRepository, otherRuleId int) error {
	otherAsDiskRepo := other.(*StaleSeriesImprovedDiskRepository)
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

func (r *StaleSeriesImprovedDiskRepository) GetNumberOfRules() int {
	return r.numberOfRules
}

func (r *StaleSeriesImprovedDiskRepository) GetRawValues() []map[string]labels.Labels {
	data := make([]map[string]labels.Labels, r.numberOfRules)
	for i := 0; i < r.numberOfRules; i++ {
		if r.files[i] == "" {
			continue
		}
		data[i] = make(map[string]labels.Labels)
		r.ScanKeys(i, func(key string, valueFunc func() labels.Labels) {
			data[i][key] = valueFunc()
		})
	}
	return data
}

/////////////////// private methods

func labelsToBytes(labels labels.Labels) []byte {
	//bytes, _ := labels.MarshalJSON()
	//return bytes
	return labels.Bytes(nil)
}

func labelsFromBytes(bytes []byte) labels.Labels {
	labels := &labels.Labels{}
	labels.FromBytes(bytes)
	return *labels
}

func (r *StaleSeriesDiskRepository) ensureHasFolder(ruleId int) {
	if r.files[ruleId] == "" {
		r.files[ruleId] = filepath.Join(r.baseDir, fmt.Sprintf("rule%d", ruleId))
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
