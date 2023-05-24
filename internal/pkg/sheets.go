package pkg

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/prometheus/common/model"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	svc           *sheets.Service
	SpreadsheetID string
	SheetID       int
	rateLimiter   *rate.Limiter
}

func NewClient(spreadsheetID string, sheetID int) *Client {
	return &Client{
		SpreadsheetID: spreadsheetID,
		SheetID:       sheetID,
		rateLimiter:   rate.NewLimiter(rate.Every(1*time.Second), 60),
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

	rows := make([]*sheets.RowData, len(samples))

	now := time.Now()

	for i, s := range samples {

		values := sampleToCells(now, s)

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

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return err
	}
	call := c.svc.Spreadsheets.BatchUpdate(c.SpreadsheetID, req)
	_, err := call.Context(ctx).Do()
	return err
}
func sampleToCells(now time.Time, s *model.Sample) (cells []*sheets.CellData) {

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			NumberValue: ref(float64(now.UTC().Unix())),
		},
	})

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			NumberValue: ref(float64(s.Timestamp.Time().UTC().Unix())),
		},
	})

	name, _ := s.Metric[model.MetricNameLabel]

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			StringValue: ref(string(name)),
		},
	})

	valueFloat := ref(float64(s.Value))
	if math.IsNaN(*valueFloat) {
		valueFloat = nil
	}

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			NumberValue: valueFloat,
		},
	})

	dims := []string{}
	for k, v := range s.Metric {
		if k == model.MetricNameLabel {
			continue
		}
		dims = append(dims, fmt.Sprintf("%s: %s", k, v))
	}
	sortedDims := sort.StringSlice(dims)
	sortedDims.Sort()

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			StringValue: ref(strings.Join(sortedDims, "\n")),
		},
	})

	return cells

}

type Config struct {
	Metrics     []string
	RetireAfter time.Duration
}

func (c *Client) GetConfig(ctx context.Context) (*Config, error) {
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return nil, err
	}
	req := c.svc.Spreadsheets.Values.Get(c.SpreadsheetID, "Config!A:B")
	res, err := req.Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	conf := Config{}
	for _, row := range res.Values {
		if len(row) < 2 {
			continue
		}
		key := fmt.Sprintf("%s", row[0])
		strVal := fmt.Sprintf("%s", row[1])
		switch strings.ToUpper(key) {
		case "METRICS":
			val := strings.Split(strings.ToLower(strVal), "\n")
			conf.Metrics = val
		case "RETIRE_AFTER":
			if val, err := time.ParseDuration(strings.ToLower(strVal)); err != nil {
				conf.RetireAfter = 10 * time.Minute
			} else {
				conf.RetireAfter = val
			}

		}
	}

	return &conf, nil
}
func (c *Client) RetireMetrics(ctx context.Context) (int, error) {

	sortReq := c.svc.Spreadsheets.BatchUpdate(c.SpreadsheetID, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				SortRange: &sheets.SortRangeRequest{
					Range: &sheets.GridRange{},
					SortSpecs: []*sheets.SortSpec{
						{
							DimensionIndex: 0,
							SortOrder:      "ASCENDING",
						},
					},
				},
			},
		},
		ResponseIncludeGridData: true,
	})
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return 0, err
	}
	_, err := sortReq.Context(ctx).Do()
	if err != nil {
		return 0, err
	}

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return 0, err
	}
	res, err := c.svc.Spreadsheets.Values.Get(c.SpreadsheetID, "Internal!B1").Context(ctx).Do()
	if err != nil {
		return 0, err
	}

	if len(res.Values) == 0 || len(res.Values[0]) == 0 {
		return 0, nil
	}

	val := res.Values[0][0]
	valStr := fmt.Sprintf("%s", val)

	switch valStr {
	case "#N/A":
		return 0, nil
	}

	rowNumber, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, err
	}

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return 0, err
	}
	_, err = c.svc.Spreadsheets.BatchUpdate(c.SpreadsheetID, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				DeleteDimension: &sheets.DeleteDimensionRequest{
					Range: &sheets.DimensionRange{
						Dimension: "ROWS",
						EndIndex:  int64(rowNumber),
					},
					ForceSendFields: nil,
					NullFields:      nil,
				},
			},
		},
	}).Context(ctx).Do()
	if err != nil {
		return 0, nil
	}

	return rowNumber, nil
}
