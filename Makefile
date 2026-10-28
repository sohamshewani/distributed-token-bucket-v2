CXX = g++
CXXFLAGS = -O3 -Wall -Wextra -std=c++17 -pthread
LDFLAGS = -pthread

SRC_DIR = src
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmarks

TARGET = distributed-token-bucket
TEST_TARGET = run_tests
BENCH_TARGET = run_benchmarks

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.cpp)
BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.cpp)

.PHONY: all build test clean benchmark

all: build

build: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS) | $(BUILD_DIR)
	@if [ -n "$(OBJS)" ]; then \
		$(CXX) $(CXXFLAGS) $(OBJS) -o $@ $(LDFLAGS); \
	else \
		echo "No source files found in $(SRC_DIR). Creating dummy binary."; \
		mkdir -p $(BUILD_DIR); \
		touch $@; \
		chmod +x $@; \
	fi

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

test: build
	@mkdir -p $(BUILD_DIR)
	@if [ -d "$(TEST_DIR)" ] && [ -n "$$(ls -A $(TEST_DIR) 2>/dev/null)" ]; then \
		$(CXX) $(CXXFLAGS) $(TEST_DIR)/*.cpp $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BUILD_DIR)/$(TEST_TARGET) $(LDFLAGS); \
		./$(BUILD_DIR)/$(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR). Skipping test target."; \
	fi

benchmark: build
	@mkdir -p $(BUILD_DIR)
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$$(ls -A $(BENCH_DIR) 2>/dev/null)" ]; then \
		$(CXX) $(CXXFLAGS) $(BENCH_DIR)/*.cpp $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BUILD_DIR)/$(BENCH_TARGET) $(LDFLAGS); \
		./$(BUILD_DIR)/$(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping benchmark target."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)