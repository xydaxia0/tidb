// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
)

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (s *testSuite) SetUpSuite(c *C) {
}

type mockSelectResult struct {
	data []mockPartialResult
	idx  int
}

type mockItem struct {
	handle int64
	data   []types.Datum
}

type mockPartialResult struct {
	data []mockItem
	idx  int
}

func (mock *mockSelectResult) Next() (xapi.PartialResult, error) {
	if mock.idx >= len(mock.data) {
		return nil, nil
	}

	idx := mock.idx
	mock.idx++
	return &mock.data[idx], nil
}

func (mock *mockSelectResult) SetFields(fields []*types.FieldType) {
}
func (mock *mockSelectResult) Close() error { return nil }
func (mock *mockSelectResult) Fetch()       {}

func (mock *mockPartialResult) Next() (handle int64, data []types.Datum, err error) {
	if mock.idx >= len(mock.data) {
		return 0, nil, nil
	}

	idx := mock.idx
	mock.idx++
	item := mock.data[idx]
	return item.handle, item.data, nil
}

func (mock *mockPartialResult) Close() error { return nil }

type mockWorkPool struct {
}

func (_ mockWorkPool) AddTask(task *lookupTableTask) {
	task.rows = make([]*Row, len(task.handles))
	for i, v := range task.handles {
		task.rows[i] = &Row{
			Data: []types.Datum{types.NewDatum(v)},
		}
	}
	task.doneCh <- nil
}

func (_ mockWorkPool) AddWorker()       {}
func (_ mockWorkPool) Concurrency() int { return 99999 }
func (_ mockWorkPool) Close()           {}

func (s *testSuite) TestFetchHandles(c *C) {
	defer testleak.AfterTest(c)()

	prs := []mockPartialResult{
		mockPartialResult{
			[]mockItem{
				mockItem{1, []types.Datum{types.NewDatum(1)}},
				mockItem{2, []types.Datum{types.NewDatum(2)}},
				mockItem{3, []types.Datum{types.NewDatum(3)}},
				mockItem{4, []types.Datum{types.NewDatum(4)}},
				mockItem{5, []types.Datum{types.NewDatum(5)}},
				mockItem{6, []types.Datum{types.NewDatum(6)}},
			},
			0,
		},
		mockPartialResult{
			[]mockItem{
				mockItem{13, []types.Datum{types.NewDatum(13)}},
				mockItem{14, []types.Datum{types.NewDatum(14)}},
				mockItem{15, []types.Datum{types.NewDatum(15)}},
				mockItem{16, []types.Datum{types.NewDatum(16)}},
			},
			0,
		},
		mockPartialResult{
			[]mockItem{
				mockItem{21, []types.Datum{types.NewDatum(21)}},
				mockItem{23, []types.Datum{types.NewDatum(23)}},
				mockItem{24, []types.Datum{types.NewDatum(24)}},
				mockItem{25, []types.Datum{types.NewDatum(25)}},
			},
			0,
		},
	}
	idxResult := &mockSelectResult{
		data: prs,
	}

	e := &XSelectIndexExec{
		indexPlan: &plan.PhysicalIndexScan{
			OutOfOrder: true,
		},
	}
	ch := make(chan *lookupTableTask, 50)
	e.fetchHandles(idxResult, ch, mockWorkPool{})
	var prev *Row
	for task := range ch {
		for {
			row, err := task.getRow()
			c.Assert(err, IsNil)
			if row == nil {
				break
			}

			if prev == nil {
				prev = row
			} else {
				cmp, err := row.Data[0].CompareDatum(prev.Data[0])
				c.Assert(err, IsNil)
				c.Assert(cmp > 0, IsTrue)
			}
		}
	}
}
