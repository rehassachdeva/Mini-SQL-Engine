#include<bits/stdc++.h>
using namespace std;

#include "SQLParser.h"

#include "sqlhelper.h"

#define BEGAN_TABLE 0
#define RUN_TABLE 1
#define ENDED_TABLE 2

#define AGGMAX "max"
#define AGGMIN "min"
#define AGGAVG "avg"
#define AGGSUM "sum"

int numTables=0;
bool fAnd;
bool fOr;
bool fOne;
hsql::SQLParserResult* result;
hsql::SQLStatement* stmt;
hsql::SelectStatement* selectStmt;
hsql::TableRef* table;
hsql::Expr* whereClause;
map<string, vector<string> > tables;
vector<hsql::Expr*> attributes;
map< hsql::Expr*, int> attrNum;
map<string, vector<hsql::Expr*> > attrGroups;
vector< pair<string, vector<hsql::Expr*> > > attrGrpsVec;
map<string, vector< pair<int, hsql::Expr*> > > conditions;
map<string, bool> outputs;

vector<string> split(const string &s, char delim) {
    stringstream ss(s);
    string item;
    vector<string> tokens;
    while (getline(ss, item, delim)) {
        tokens.push_back(item);
    }
    return tokens;
}

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

bool validateSyntax() {
    if(!result->isValid) {
        cout<<"Invalid SQL!\n";
        cout<<result->errorMsg<<endl;
        cout<<"Error line: "<<result->errorLine+1<<endl;
        cout<<"Error column: "<<result->errorColumn+1<<endl;
        return false;    
    }
    return true;
}

bool validateScope(string query) {
    if(stmt->type()!=hsql::kStmtSelect) {
        cout<<"Sorry! Query \""<<query<<"\" is out of scope!\n";
        return false;
    }
    return true;
}

bool checkExistsSingle() {
    if(table->type==hsql::kTableName and tables.find(table->name)==tables.end()) {
        cout<<"Table "<<table->name<<" not found!\n";
        return false;
    }
    return true;
}

bool checkExistsMultiple() {
    if(table->type==hsql::kTableCrossProduct) {
        for (hsql::TableRef* tbl : *table->list) {
            if(tables.find(tbl->name)==tables.end()) {
                cout<<"Table "<<tbl->name<<" not found!\n";
                return false;
            }
        }
    }
    return true;
}

bool checkAmbiguityAndPresence() {
    if(attributes.size()==1 and attributes[0]->type==hsql::kExprStar) return true;
    if(attributes.size()==1 and attributes[0]->type==hsql::kExprFunctionRef) return true;
    for(int i=0;i<attributes.size();i++) {
        if(attributes[i]->table) {
            vector<string>::iterator pos=find(tables[attributes[i]->table].begin(),
                    tables[attributes[i]->table].end(),
                    attributes[i]->name);
            if(pos==tables[attributes[i]->table].end()) {
                cout<<"Column "<<attributes[i]->name<<" in table "<<attributes[i]->table<<" not found!\n";
                return false;
            }
            else {
                attrNum[attributes[i]]=pos-tables[attributes[i]->table].begin();
                attrGroups[attributes[i]->table].push_back(attributes[i]);
            }
        }
        else {
            int cntNumTables=0;
            if(table->type==hsql::kTableCrossProduct) {
                for (hsql::TableRef* tbl : *table->list) {
                    vector<string>::iterator pos=find(tables[tbl->name].begin(),
                            tables[tbl->name].end(),
                            attributes[i]->name);
                    if(pos!=tables[tbl->name].end()) {
                        cntNumTables++;
                        attributes[i]->table=tbl->name;
                        attrNum[attributes[i]]=pos-tables[tbl->name].begin();
                        attrGroups[(string)tbl->name].push_back(attributes[i]);
                    }
                }
            }
            else if(table->type==hsql::kTableName) {
                vector<string>::iterator pos=find(tables[table->name].begin(),
                        tables[table->name].end(),
                        attributes[i]->name);
                if(pos!=tables[table->name].end()) {
                    cntNumTables++;
                    attributes[i]->table=table->name;
                    attrNum[attributes[i]]=pos-tables[table->name].begin();
                    attrGroups[(string)table->name].push_back(attributes[i]);
                }
            }
            if(cntNumTables>1) {
                cout<<"Ambiguous column "<<attributes[i]->name<<"!\n";
                return false;
            }
            else if(cntNumTables==0) {
                cout<<"Column "<<attributes[i]->name<<" not found!\n";
                return false;
            }
        }
    }
    return true;
}

bool checkExistsWhere(hsql::Expr* expr, hsql::Expr* parent) {
    if(expr->type==hsql::kExprColumnRef) {
        if(expr->table) {
            if(tables.find(expr->table)==tables.end()) {
                cout<<"Table "<<expr->table<<" not found!\n";
                return false;
            }
            vector<string>::iterator pos=find(tables[expr->table].begin(), 
                    tables[expr->table].end(), expr->name);

            if(pos!=tables[expr->table].end())
                conditions[expr->table].push_back(make_pair(pos-tables[expr->table].begin(), parent));

            else {
                cout<<"Column "<<expr->name<<" in table "<<expr->table<<" not found!\n";
                return false;
            }
        }
        else if(!expr->table) {
            int cntNumTables=0;
            if(table->type==hsql::kTableCrossProduct) {
                for (hsql::TableRef* tbl : *table->list) {
                    vector<string>::iterator pos=find(tables[tbl->name].begin(),
                            tables[tbl->name].end(),
                            expr->name);
                    if(pos!=tables[tbl->name].end()) {
                        cntNumTables++;
                        conditions[tbl->name].push_back(make_pair(pos-tables[tbl->name].begin(), parent));
                    }
                }
            }
            else if(table->type==hsql::kTableName) {
                vector<string>::iterator pos=find(tables[table->name].begin(),
                        tables[table->name].end(),
                        expr->name);
                if(pos!=tables[table->name].end()) {
                    cntNumTables++;
                    conditions[table->name].push_back(make_pair(pos-tables[table->name].begin(), parent));

                }
            }
            if(cntNumTables>1) {
                cout<<"Ambiguous column "<<expr->name<<"!\n";
                return false;
            }
            else if(cntNumTables==0) {
                cout<<"Column "<<expr->name<<" not found!\n";
                return false;
            }
        }
    }
    return true;
}

bool checkWhereSemantics() {
    if(!whereClause) return true;
    fAnd=false;
    fOr=false;
    fOne=false;
    if(whereClause->type==hsql::kExprOperator and
            (whereClause->op_type==hsql::Expr::SIMPLE_OP or
             whereClause->op_type==hsql::Expr::NOT_EQUALS or
             whereClause->op_type==hsql::Expr::LESS_EQ or
             whereClause->op_type==hsql::Expr::GREATER_EQ or
             whereClause->op_type==hsql::Expr::UMINUS)
      ) {
        fOne=true;

        if(!checkExistsWhere(whereClause->expr, whereClause)) return false;
        if(!checkExistsWhere(whereClause->expr2, whereClause)) return false;

    }

    if(whereClause and
            whereClause->type==hsql::kExprOperator and
            whereClause->op_type==hsql::Expr::AND) fAnd=true;

    if(whereClause and
            whereClause->type==hsql::kExprOperator and
            whereClause->op_type==hsql::Expr::OR) fOr=true;

    if(fAnd or fOr) {
        if(!checkExistsWhere(whereClause->expr->expr, whereClause->expr)) return false;
        if(!checkExistsWhere(whereClause->expr->expr2, whereClause->expr)) return false;
        if(!checkExistsWhere(whereClause->expr2->expr, whereClause->expr2)) return false;
        if(!checkExistsWhere(whereClause->expr2->expr2, whereClause->expr2)) return false;
    }

    return true;
}

bool executeQueryTypeA() {
    if(table->type==hsql::kTableName and 
            attributes.size()==1 and 
            attributes[0]->type==hsql::kExprStar) {

        ifstream tableFile((string)table->name+".csv");
        int i=0;
        string line;
        for(i=0;i<tables[table->name].size()-1;i++)
            cout<<table->name<<"."<<tables[table->name][i]<<",";
        cout<<table->name<<"."<<tables[table->name][i]<<endl;

        while(!tableFile.eof()) {
            tableFile>>line;
            cout<<line<<endl;
        }
        return true;
    }
    return false;
}

bool executeQueryTypeB() {
    if(table->type==hsql::kTableName and
            attributes.size()==1 and
            attributes[0]->type==hsql::kExprFunctionRef) {

        string attr=attributes[0]->expr->name;
        vector<string>::iterator it;
        it=find(tables[table->name].begin(),tables[table->name].end(),attr);

        if(it==tables[table->name].end()) {
            cout<<"Column "<<attr<<" not found!\n";
            return true;
        }

        int attrNum=it-tables[table->name].begin();

        string line;

        ifstream tableFile((string)table->name+".csv");

        long long sum=0,numLines=0,mx=LLONG_MIN,mn=LLONG_MAX;

        string aggType=(*selectStmt->selectList)[0]->name;

        transform(aggType.begin(), aggType.end(), aggType.begin(),
                [](unsigned char c) { return std::tolower(c); });

        if(aggType!=AGGMAX and aggType!=AGGMIN and aggType!=AGGAVG and aggType!=AGGSUM) {
            cout<<"Aggregate Function "<<aggType<<" not found!\n";
            cout<<"Available aggregate functions are max, min, avg and sum\n";
            return true;
        }

        while(!tableFile.eof()) {
            tableFile>>line;
            numLines++;
            vector<string> items=split(line,',');

            mx=max(stoll(items[attrNum]),mx);
            mn=min(stoll(items[attrNum]),mn);
            sum+=stoll(items[attrNum]);
        }

        if(aggType==AGGMAX) cout<<mx<<endl;
        else if(aggType==AGGMIN) cout<<mn<<endl;
        else if(aggType==AGGAVG) cout<<(double)sum/numLines<<endl;
        else if(aggType==AGGSUM) cout<<sum<<endl;

        return true;
    }
    return false;

}

int checkConditions(string curTable, vector<string> items) {
    int numTrue=0;
    if(fOr==false and fAnd==false and fOne==false) return 0;
    if(conditions.find(curTable)==conditions.end()) return 0;
    vector< pair<int, hsql::Expr*> >::iterator it;
    for(it=conditions[curTable].begin();it!=conditions[curTable].end();it++) {
        hsql::Expr* condition=(*it).second;
        int idx=(*it).first;
        if(condition->op_char=='=' and stoll(items[idx])==condition->expr2->ival) numTrue++;
        else if(condition->op_char=='>' and stoll(items[idx])>condition->expr2->ival) numTrue++;
        else if(condition->op_char=='<' and stoll(items[idx])<condition->expr2->ival) numTrue++;
        else if(condition->op_type==hsql::Expr::NOT_EQUALS and stoll(items[idx])!=condition->expr2->ival) numTrue++;
        else if(condition->op_type==hsql::Expr::LESS_EQ and stoll(items[idx])<=condition->expr2->ival) numTrue++;
        else if(condition->op_type==hsql::Expr::GREATER_EQ and stoll(items[idx])>=condition->expr2->ival) numTrue++;
        else if(condition->expr2 and condition->expr2->op_type==hsql::Expr::UMINUS and -stoll(items[idx])==condition->expr2->expr->ival) numTrue++;
    }
    return numTrue;
}

void executeQueryTypeCDEUtil(string pref, int start, int end, int numTrue) {

    string curTable=attrGrpsVec[start].first;
    vector<hsql::Expr* > curCols=attrGrpsVec[start].second;

    ifstream tableFile(curTable+".csv");

    string line;

    string curPref;
    int numTrueTemp;
    while(!tableFile.eof()) {
        numTrueTemp=numTrue;
        curPref=pref;
        tableFile>>line;
        vector<string> items=split(line,',');
        numTrueTemp+=checkConditions(curTable, items);
        for(int i=0;i<curCols.size();i++) {
            curPref+=items[attrNum[curCols[i]]];
            if(end==start and i==curCols.size()-1) curPref+="\n";
            else curPref+=",";
        }
        if(start==end) {
            if(fAnd and numTrueTemp!=2) continue;
            if((fOne or fOr) and numTrueTemp!=1) continue;
            if(!selectStmt->selectDistinct or outputs[curPref]==false)
                cout<<curPref;
            outputs[curPref]=true;
        }
        if(start<end) executeQueryTypeCDEUtil(curPref, start+1,end,numTrueTemp);            
    }
}

bool executeQueryTypeCDE() {

    if(table->type==hsql::kTableCrossProduct or table->type==hsql::kTableName) {

        map<string, vector<hsql::Expr*> >::iterator it;

        for(it=attrGroups.begin();it!=attrGroups.end();it++) {
            vector<hsql::Expr*>::iterator ita;
            for(ita=((*it).second).begin();ita!=((*it).second).end();ita++) {
                cout<<(*it).first<<"."<<(*ita)->name;
                if(it==(--(attrGroups.end())) and ita==((*it).second).end()-1)
                    cout<<endl;
                else cout<<",";
            }
            attrGrpsVec.push_back(make_pair((*it).first,(*it).second));
        }
        executeQueryTypeCDEUtil("", 0, attrGrpsVec.size()-1, 0);
        return true;
    }
    return false;
}

bool executeQueryTypeF() {
    if(whereClause and
            whereClause->type==hsql::kExprOperator and
            (whereClause->op_type==hsql::Expr::SIMPLE_OP) and
            table->type==hsql::kTableCrossProduct and
            (*table->list).size()==2 and
            whereClause->expr->type==hsql::kExprColumnRef and
            whereClause->expr2->type==hsql::kExprColumnRef) {

        string table1=whereClause->expr->table;
        string table2=whereClause->expr2->table;

        vector<int> cols1;
        vector<int> cols2;


        if(attributes.size()==1 and attributes[0]->type==hsql::kExprStar) {
            for(int i=0;i<tables[table1].size();i++)
                cols1.push_back(i);
            for(int i=0;i<tables[table2].size();i++)
                cols2.push_back(i);
        }
        else {
            for(int i=0;i<attrGroups[table1].size();i++)
                cols1.push_back(find(tables[table1].begin(),tables[table1].end(),attrGroups[table1][i]->name)-tables[table1].begin());
            for(int i=0;i<attrGroups[table2].size();i++)
                cols2.push_back(find(tables[table2].begin(),tables[table2].end(),attrGroups[table2][i]->name)-tables[table2].begin());
        }

        ifstream tableFile1(table1+".csv");

        int condCol1=find(tables[table1].begin(), tables[table1].end(), whereClause->expr->name)-tables[table1].begin();
        int condCol2=find(tables[table2].begin(), tables[table2].end(), whereClause->expr2->name)-tables[table2].begin();

        vector<string> output;

        for(int i=0;i<cols1.size()-1;i++)
        	cout<<table1<<"."<<tables[table1][cols1[i]]<<",";
        cout<<table1<<"."<<tables[table1][cols1.size()-1];
        if(cols2.size()>0)
        	cout<<",";
        for(int i=0;i<cols2.size()-1;i++)
        	cout<<table2<<"."<<tables[table2][cols2[i]]<<",";
        cout<<table2<<"."<<tables[table2][cols2.size()-1]<<endl;


        while(!tableFile1.eof()) {
            ifstream tableFile2(table2+".csv");
            string line1;
            tableFile1>>line1;
            vector<string> items1=split(line1,',');
            while(!tableFile2.eof()) {
            	output.clear();
                string line2;
                tableFile2>>line2;
                vector<string> items2=split(line2,',');
                if(items1[condCol1]!=items2[condCol2]) continue;
                for(int i=0;i<cols1.size();i++)
                    output.push_back(items1[cols1[i]]);
                for(int i=0;i<cols2.size();i++)
                    output.push_back(items2[cols2[i]]);
                for(int i=0;i<output.size()-1;i++)
                    cout<<output[i]<<",";
                cout<<output[output.size()-1]<<endl;

            }
        }
        return true;
    }
    return false;
}

int main(int argc, char *argv[]) {

    if(argc<=1) {
        cout<<"Usage: ./minisql \"SELECT * FROM test;\"\n";
        return -1;
    }
    const string input=argv[1];

    vector<string> queries=split(input, ';');

    for(string query : queries) {

        result=hsql::SQLParser::parseSQLString(query);

        if(!validateSyntax()) continue;

        stmt=result->getStatement(0);

        selectStmt=(hsql::SelectStatement*)stmt;

        if(!validateScope(query)) continue;

        parseMetadata();

        attributes=(*selectStmt->selectList);

        table=selectStmt->fromTable;

        if(!checkExistsSingle()) continue;      

        if(!checkExistsMultiple()) continue;

        if(!checkAmbiguityAndPresence()) continue;

        whereClause=selectStmt->whereClause;

        if(!checkWhereSemantics()) continue;

        if(executeQueryTypeF()) continue;

        if(executeQueryTypeA()) continue;

        if(executeQueryTypeB()) continue;

        if(executeQueryTypeCDE()) continue;

    }    
    return 0;
}
