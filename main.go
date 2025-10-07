package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
)

// get the bucket and key from an S3 URL: parseS3 parses an S3 URL of the form s3://bucket/key and returns the bucket and key.
func parseS3(url string) (string, string) {
	//strings.TrimPrefix: removes the "s3://" prefix from the URL.
	trim := strings.TrimPrefix(url, "s3://")
	//strings.IndexByte: finds the index of the first '/' character in the trimmed string.
	i := strings.IndexByte(trim, '/')
	return trim[:i], trim[i+1:]
}

// use AWS SDK to create S3 client, uploader, and downloader
func mustAws() (*s3.Client, *manager.Uploader, *manager.Downloader) {
	// config.LoadDefaultConfig: loads the default AWS configuration, specifying the region as "us-east-1".
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		panic(err)
	}
	// s3.NewFromConfig: creates a new S3 client using the loaded configuration.
	client := s3.NewFromConfig(cfg)
	// manager.NewUploader and manager.NewDownloader: create an uploader and downloader for S3 operations.
	return client, manager.NewUploader(client), manager.NewDownloader(client)
}

func main() {
	mode := os.Getenv("MODE") // splitter | mapper | reducer

	// initialize a gin router
	r := gin.Default()

	_, uploader, downloader := mustAws()

	// -------- SPLITTER --------
	//split input file into smaller chunks and upload to S3.
	r.GET("/split", func(c *gin.Context) {

		// if ip is splitter task but mode is not "splitter", return an error
		if mode != "splitter" {
			c.String(http.StatusBadRequest, "This task is not a splitter")
			return
		}

		// get input parameters from query string
		// input_s3: S3 URL of the input file to be split
		// out_prefix: S3 prefix for the output chunks
		// parts: number of parts to split into 3
		input := c.Query("input_s3")
		parts := 3
		prefix := c.Query("out_prefix")

		// parse the S3 URL to get the bucket and key
		// parseS3: function to parse S3 URL and return bucket and key
		bucket, key := parseS3(input)

		// Creates a memory buffer to hold the downloaded file content.
		buf := manager.NewWriteAtBuffer([]byte{})
		// Downloads the file from S3 into the buffer.
		// downloader.Download: downloads the file from S3 into the buffer.
		//context.Background() is an empty context. and means no timeout or cancellation.
		_, err := downloader.Download(context.Background(), buf,
			&s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			c.String(500, err.Error())
			return
		}
		// Converts the buffer content to a string and splits it into lines.
		content := string(buf.Bytes())
		lines := strings.Split(content, "\n")

		// we use ceiling division to calculate the size of each chunk and split the lines into chunks.
		chunkSize := (len(lines) + parts - 1) / parts
		urls := []string{}
		for i := 0; i < parts; i++ {
			start := i * chunkSize
			end := (i + 1) * chunkSize
			if end > len(lines) {
				end = len(lines)
			}
			chunk := strings.Join(lines[start:end], "\n")

			// Creates the output filename. like prefix/chunk-0.txt.
			outKey := fmt.Sprintf("%schunk-%d.txt", strings.TrimSuffix(prefix, "/")+"/", i)

			// Uploads the chunk to S3 using the uploader.
			_, err := uploader.Upload(context.Background(),
				&s3.PutObjectInput{
					Bucket: &bucket,
					Key:    &outKey,
					Body:   strings.NewReader(chunk),
				})
			if err != nil {
				c.String(500, err.Error())
				return
			}
			// Appends the S3 URL of the uploaded chunk to the urls slice. like s3://mybucket/results/chunk-0.txt.
			urls = append(urls, fmt.Sprintf("s3://%s/%s", bucket, outKey))
		}
		c.JSON(200, urls)
	})

	// -------- MAPPER --------
	// read a chunk from S3, count word occurrences, and write the result back to S3.
	r.GET("/map", func(c *gin.Context) {
		if mode != "mapper" {
			c.String(http.StatusBadRequest, "This task is not a mapper")
			return
		}

		// Reads query parameter chunk_s3, which is the S3 URL of one chunk file. like s3://mybucket/results/chunk-0.txt
		chunkS3 := c.Query("chunk_s3")
		// Reads query parameter out_s3, which is the S3 URL where the mapper should write its output. like s3://mybucket/results/map-output-0.json
		outS3 := c.Query("out_s3")

		// parse the S3 URL to get the bucket and key
		bucket, key := parseS3(chunkS3)
		// Creates a memory buffer to hold the downloaded chunk file.
		buf := manager.NewWriteAtBuffer([]byte{})

		// Downloads the chunk file from S3 into a buffer.
		_, err := downloader.Download(context.Background(), buf,
			&s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			c.String(500, err.Error())
			return
		}

		// Converts the buffer content to a string.
		text := string(buf.Bytes())
		// Uses a regular expression to find all words in the text and counts their occurrences.
		re := regexp.MustCompile(`[A-Za-z0-9]+`)
		counts := map[string]int{}
		// Convert everything to lowercase and count occurrences
		for _, w := range re.FindAllString(strings.ToLower(text), -1) {
			counts[w]++
		}

		// Marshals the word count map to JSON.
		js, _ := json.Marshal(counts)

		// Uploads the JSON result to the specified S3 output location.
		outBucket, outKey := parseS3(outS3)
		_, err = uploader.Upload(context.Background(),
			&s3.PutObjectInput{
				Bucket: &outBucket,
				Key:    &outKey,
				Body:   strings.NewReader(string(js)),
			})
		if err != nil {
			c.String(500, err.Error())
			return
		}
		c.JSON(200, gin.H{"output": outS3})
	})

	// -------- REDUCER --------
	// read multiple mapper outputs from S3, aggregate the word counts, and write the final result back to S3.
	r.GET("/reduce", func(c *gin.Context) {
		if mode != "reducer" {
			c.String(http.StatusBadRequest, "This task is not a reducer")
			return
		}

		// Reads query parameter in, which can appear multiple times to specify multiple S3 URLs of mapper output files. /reduce?in=s3://bucket/map-0.json&in=s3://bucket/map-1.json&out_s3=s3://bucket/final.json
		inputs := c.QueryArray("in")
		// Reads query parameter out_s3, which is the S3 URL where the reducer should write its final output.
		outS3 := c.Query("out_s3")
		// Initializes a map to hold the aggregated word counts.
		total := map[string]int{}

		for _, in := range inputs {
			// get each mapper bucket and key from S3
			bucket, key := parseS3(in)
			// download each mapper output into a buffer
			buf := manager.NewWriteAtBuffer([]byte{})
			_, err := downloader.Download(context.Background(), buf,
				&s3.GetObjectInput{Bucket: &bucket, Key: &key})
			if err != nil {
				c.String(500, err.Error())
				return
			}
			// unmarshal the JSON content of each mapper output into a map and aggregate the counts into the total map.
			m := map[string]int{}
			json.Unmarshal(buf.Bytes(), &m)
			// aggregate the counts into the total map.
			for k, v := range m {
				total[k] += v
			}
		}

		// convert the total map to JSON and upload it to the specified S3 output location.
		js, _ := json.Marshal(total)
		outBucket, outKey := parseS3(outS3)
		_, err := uploader.Upload(context.Background(),
			&s3.PutObjectInput{
				Bucket: &outBucket,
				Key:    &outKey,
				Body:   strings.NewReader(string(js)),
			})
		if err != nil {
			c.String(500, err.Error())
			return
		}
		c.JSON(200, gin.H{"output": outS3})
	})

	r.Run(":8080")
}
