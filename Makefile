
CFLAGS = -std=c++11 -lstdc++ -Wall -I./libs/sqlparser/ -L./libs/

all:
	$(CXX) $(CFLAGS) minisql.cpp -o minisql -lsqlparser

