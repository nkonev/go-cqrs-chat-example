package utils

import (
	"fmt"
	"net/url"
	"strconv"
	"time"
)

const maxSize = 100
const DefaultSize = 20
const DefaultPage = 0
const DefaultOffset = 0

func ToString(in any) string {
	return fmt.Sprintf("%v", in)
}

func ParseInt64(s string) (int64, error) {
	if i, err := strconv.ParseInt(s, 10, 64); err != nil {
		return 0, fmt.Errorf("unable to parse int: %v", err)
	} else {
		return i, nil
	}
}

func ParseInt64Nullable(s string) *int64 {
	if i, err := strconv.ParseInt(s, 10, 64); err != nil {
		return nil
	} else {
		return &i
	}
}

func GetBoolean(s string) bool {
	if parseBool, err := strconv.ParseBool(s); err != nil {
		return false
	} else {
		return parseBool
	}
}

func GetBooleanNullable(s string) *bool {
	if parseBool, err := strconv.ParseBool(s); err != nil {
		return nil
	} else {
		return &parseBool
	}
}

func GetBooleanOr(s string, def bool) bool {
	v := GetBooleanNullable(s)
	if v != nil {
		return *v
	}
	return def
}

func GetTimeNullable(s string) *time.Time {
	time1, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	return &time1
}

func GetSliceWithout(exception int64, inputData []int64) []int64 {
	ret := []int64{}
	for _, v := range inputData {
		if v != exception {
			ret = append(ret, v)
		}
	}
	return ret
}

func GetSliceWithoutSlice(exception []int64, inputData []int64) []int64 {
	remaining := make([]int64, len(inputData))
	copy(remaining, inputData)
	for _, toDeleteId := range exception {
		remaining = GetSliceWithout(toDeleteId, remaining)
	}
	return remaining
}

func StringToUrl(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

func FixPage(page int64) int64 {
	if page < 0 {
		return DefaultPage
	} else {
		return page
	}
}

func FixPageString(page string) int64 {
	atoi, err := ParseInt64(page)
	if err != nil {
		return DefaultPage
	} else {
		return FixPage(atoi)
	}
}

func FixSize(size int32) int32 {
	if size > maxSize || size < 1 {
		return DefaultSize
	} else {
		return size
	}
}

func FixSizeString(size string) int32 {
	atoi, err := strconv.Atoi(size)
	if err != nil {
		return DefaultSize
	} else {
		return FixSize(int32(atoi))
	}

}

func GetOffset(page int64, size int32) int64 {
	return page * int64(size)
}
