package pkg

import (
	"context"
	"encoding/base64"
	"github.com/prometheus/common/model"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"math"
	"time"
)

type Client struct {
	svc           *sheets.Service
	SpreadsheetID string
	SheetID       int
	rateLimiter   *rate.Limiter
	extraLabels   *labelsColumns
}

func NewClient(spreadsheetID string, sheetID int) *Client {
	return &Client{
		SpreadsheetID: spreadsheetID,
		SheetID:       sheetID,
		rateLimiter:   rate.NewLimiter(rate.Every(1*time.Second), 60),
		extraLabels:   &labelsColumns{},
	}
}

func (c *Client) Authenticate(ctx context.Context, base64Key string) error {
	credBytes, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return err
	}

	// authenticate and get configuration
	config, err := google.JWTConfigFromJSON(credBytes, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return err
	}

	client := config.Client(ctx)

	c.svc, err = sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return err
	}
	return nil
}

func ref[T any](v T) *T {
	return &v
}

func (c *Client) Write(ctx context.Context, samples model.Samples, makeRoom bool) error {

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return err
	}

	rows := make([]*sheets.RowData, len(samples))

	for i, s := range samples {

		values := sampleToCells(s, c.extraLabels)

		rows[i] = &sheets.RowData{
			Values: values,
		}
	}

	req := &sheets.BatchUpdateSpreadsheetRequest{
		IncludeSpreadsheetInResponse: false,
	}

	if makeRoom {
		req.Requests = append(req.Requests, &sheets.Request{
			DeleteDimension: &sheets.DeleteDimensionRequest{
				Range: &sheets.DimensionRange{
					Dimension:  "ROWS",
					StartIndex: 0,
					EndIndex:   int64(len(rows)),
					SheetId:    int64(c.SheetID),
				},
			},
		})
	}

	req.Requests = append(req.Requests, &sheets.Request{
		AppendCells: &sheets.AppendCellsRequest{
			Fields:  "*",
			Rows:    rows,
			SheetId: int64(c.SheetID),
		},
	})

	call := c.svc.Spreadsheets.BatchUpdate(c.SpreadsheetID, req)
	_, err := call.Context(ctx).Do()
	return err
}
func sampleToCells(s *model.Sample, labelsMap *labelsColumns) (cells []*sheets.CellData) {

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			StringValue: ref(s.Timestamp.Time().Format(time.RFC3339Nano)),
		},
	})

	name, _ := s.Metric[model.MetricNameLabel]

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			StringValue: ref(string(name)),
		},
	})

	labelCells := make([]*sheets.CellData, 40)

	// add labels at correct column
	for k, v := range s.Metric {
		if k == model.MetricNameLabel {
			continue
		}
		cellData := &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{
				StringValue: ref(string(k) + ": " + string(v)),
			},
		}

		colIndex := labelsMap.GetOrAdd(string(k))
		if colIndex <= len(labelCells)-1 {
			labelCells[colIndex] = cellData
		}
	}

	// fill out empty cells
	for i, lv := range labelCells {
		if lv == nil {
			labelCells[i] = &sheets.CellData{
				UserEnteredValue: &sheets.ExtendedValue{
					StringValue: ref(""),
				},
			}
		}

		if i == len(*labelsMap)-1 {
			break
		}
	}

	valueFloat := ref(float64(s.Value))
	if math.IsNaN(*valueFloat) {
		valueFloat = nil
	}

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			NumberValue: valueFloat,
		},
	})

	return append(cells, labelCells...)

}

type labelsColumns []string

func (l *labelsColumns) GetOrAdd(name string) int {
	for i, n := range *l {
		if n == name {
			return i
		}
	}
	*l = append(*l, name)
	return len(*l) - 1
}
