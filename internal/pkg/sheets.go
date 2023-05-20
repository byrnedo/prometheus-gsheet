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

func (c *Client) Write(ctx context.Context, m model.Samples) error {

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return err
	}

	rows := make([]*sheets.RowData, len(m))

	for i, s := range m {
		tstampString := s.Timestamp.Time().Format(time.RFC3339Nano)
		metricString := s.Metric.String()
		valueFloat := float64(s.Value)
		var valueFloatPtr *float64
		if !math.IsNaN(valueFloat) {
			valueFloatPtr = &valueFloat
		}

		rows[i] = &sheets.RowData{
			Values: []*sheets.CellData{
				{
					UserEnteredValue: &sheets.ExtendedValue{
						StringValue: &tstampString,
					},
				},
				{
					UserEnteredValue: &sheets.ExtendedValue{
						StringValue: &metricString,
					},
				},
				{
					UserEnteredValue: &sheets.ExtendedValue{
						NumberValue: valueFloatPtr,
					},
				},
			},
		}
	}

	call := c.svc.Spreadsheets.BatchUpdate(c.SpreadsheetID, &sheets.BatchUpdateSpreadsheetRequest{
		IncludeSpreadsheetInResponse: false,
		Requests: []*sheets.Request{
			{
				AppendCells: &sheets.AppendCellsRequest{
					Fields:  "*",
					Rows:    rows,
					SheetId: int64(c.SheetID),
				},
			},
		},
	})
	_, err := call.Context(ctx).Do()
	return err
}
