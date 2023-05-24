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

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return err
	}

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

	call := c.svc.Spreadsheets.BatchUpdate(c.SpreadsheetID, req)
	_, err := call.Context(ctx).Do()
	return err
}
func sampleToCells(now time.Time, s *model.Sample) (cells []*sheets.CellData) {

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			NumberValue: ref(float64(now.Unix())),
		},
	})

	cells = append(cells, &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			NumberValue: ref(float64(s.Timestamp.Time().Unix())),
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
