##
## Makefile for vital2csv
##

SRC := vital2csv.go
TARGET := $(SRC:.go=)
TEST_DATA := VitalgramLogData.sqlite

all: $(TARGET)

$(TARGET): $(SRC)
	go build $(SRC)

test: $(TARGET)
	./$(TARGET) -d output $(TEST_DATA)
