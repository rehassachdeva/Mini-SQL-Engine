#include<bits/stdc++.h>
using namespace std;

// include the sql parser
#include "SQLParser.h"

// contains printing utilities
#include "sqlhelper.h"

#define BEGAN_TABLE 0
#define RUN_TABLE 1
#define ENDED_TABLE 2

int numTables=0;
map<string, vector<string> > tables;

void parseMetadata() {
	ifstream inFile("metadata.txt");
	string line,curTable;
	int flag=ENDED_TABLE;
	while(inFile>>line) {
		if(line=="<begin_table>") {
			flag=BEGAN_TABLE;
			numTables++;
		}
		else if(line=="<end_table>") {
			flag=ENDED_TABLE;
		}
		else if(flag==BEGAN_TABLE) {
			vector<string> attributes;
			string newTable=line;
			tables[newTable]=attributes;
			curTable=line;
			flag=RUN_TABLE;
		}
		else {
			string newAttr=line;
			tables[curTable].push_back(newAttr);
		}
	}
	inFile.close();
}

void printTables() {
	map<string, vector<string> >::iterator it;
	for(it=tables.begin();it!=tables.end();it++) {
		cout<<(*it).first<<endl;
		vector<string>::iterator it2;
		for(it2=(*it).second.begin();it2!=(*it).second.end();it2++) {
			cout<<*it2<<" ";
		}
		cout<<endl;
	}
}

int main(int argc, char *argv[]) {
	/*if(argc<=0) {
        fprintf(stderr,"Usage: ./minisql \"SELECT * FROM test;\"\n");
        return -1;
    }
    string query=argv[1];

    hsql::SQLParserResult* result=hsql::SQLParser::parseSQLString(query);

    if(result->isValid) {
        printf("Parsed successfully!\n");
        printf("Number of statements: %lu\n", result->size());

        for (hsql::SQLStatement* stmt : result->statements) {
            hsql::printStatementInfo(stmt);
        }
        return 0;
    } 
    else {
        printf("Invalid SQL!\n");
        return -1;
    }*/

    parseMetadata();
    printTables(); 

	return 0;
}
