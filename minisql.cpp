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
    if(selectStmt->selectDistinct and (*selectStmt->selectList).size()!=1) {
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

bool checkExistsWhere(hsql::Expr* expr) {
    if(expr->type==hsql::kExprColumnRef) {
        if(expr->table) {
            if(tables.find(expr->table)==tables.end()) {
                cout<<"Table "<<expr->table<<" not found!\n";
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
                    }
                }
            }
            else if(table->type==hsql::kTableName) {
                vector<string>::iterator pos=find(tables[table->name].begin(),
                        tables[table->name].end(),
                        expr->name);
                if(pos!=tables[table->name].end()) {
                    cntNumTables++;
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

        if(!checkExistsWhere(whereClause->expr)) return false;
        if(!checkExistsWhere(whereClause->expr2)) return false;

    }

    if(whereClause and
            whereClause->type==hsql::kExprOperator and
            whereClause->op_type==hsql::Expr::AND) fAnd=true;

    if(whereClause and
            whereClause->type==hsql::kExprOperator and
            whereClause->op_type==hsql::Expr::OR) fOr=true;

    if(fAnd or fOr) {
        if(!checkExistsWhere(whereClause->expr->expr)) return false;
        if(!checkExistsWhere(whereClause->expr->expr2)) return false;
        if(!checkExistsWhere(whereClause->expr2->expr)) return false;
        if(!checkExistsWhere(whereClause->expr2->expr2)) return false;
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

bool checkConditions(map<int, vector<hsql::Expr*> > conditionCols, vector<string> items, bool fAnd, bool fOr, bool fOne) {
    if(fOr==false and fAnd==false and fOne==false) return true;
    map<int, vector<hsql::Expr*> >::iterator it;
    vector<hsql::Expr*>::iterator it1;
    int numTrue=0;
    for(it=conditionCols.begin();it!=conditionCols.end();it++) {
        for(it1=(it->second).begin();it1!=(it->second).end();it1++) {
            hsql::Expr* condition=*it1;
            int idx=(*it).first;
            if(condition->op_char=='=' and stoll(items[idx])==condition->expr2->ival) numTrue++;
            else if(condition->op_char=='>' and stoll(items[idx])>condition->expr2->ival) numTrue++;
            else if(condition->op_char=='<' and stoll(items[idx])<condition->expr2->ival) numTrue++;
            else if(condition->op_type==hsql::Expr::NOT_EQUALS and stoll(items[idx])!=condition->expr2->ival) numTrue++;
            else if(condition->op_type==hsql::Expr::LESS_EQ and stoll(items[idx])<=condition->expr2->ival) numTrue++;
            else if(condition->op_type==hsql::Expr::GREATER_EQ and stoll(items[idx])>=condition->expr2->ival) numTrue++;
            else if(condition->expr2 and condition->expr2->op_type==hsql::Expr::UMINUS and -stoll(items[idx])==condition->expr2->expr->ival) numTrue++;
        }
    }
    if(fAnd and numTrue<2) return false;
    else if(numTrue==0) return false;
    return true;
}

bool executeQueryTypeC1DE() {

    if(table->type==hsql::kTableName) {
        vector<int> cols;
        map<int, vector<hsql::Expr*> > conditionCols;

        bool fAnd=false, fOr=false, fOne=false;

        if(selectStmt->whereClause and
                selectStmt->whereClause->type==hsql::kExprOperator and
                (selectStmt->whereClause->op_type==hsql::Expr::SIMPLE_OP or
                 selectStmt->whereClause->op_type==hsql::Expr::NOT_EQUALS or
                 selectStmt->whereClause->op_type==hsql::Expr::LESS_EQ or
                 selectStmt->whereClause->op_type==hsql::Expr::GREATER_EQ or
                 selectStmt->whereClause->op_type==hsql::Expr::UMINUS)
          ) {
            fOne=true;

            vector<string>::iterator it=find(tables[table->name].begin(),
                    tables[table->name].end(),
                    (string)selectStmt->whereClause->expr->name);

            if(it==tables[table->name].end()) {
                cout<<"Column "<<(string)selectStmt->whereClause->expr->name<<" not found!\n";
                return true;
            }

            conditionCols[it-tables[table->name].begin()].push_back(
                    selectStmt->whereClause);
        }

        if(selectStmt->whereClause and
                selectStmt->whereClause->type==hsql::kExprOperator and
                selectStmt->whereClause->op_type==hsql::Expr::AND) fAnd=true;

        if(selectStmt->whereClause and
                selectStmt->whereClause->type==hsql::kExprOperator and
                selectStmt->whereClause->op_type==hsql::Expr::OR) fOr=true;

        if(fAnd or fOr) {
            conditionCols[find(tables[table->name].begin(),tables[table->name].end(),(string)(selectStmt->whereClause->expr->expr->name))-tables[table->name].begin()].push_back(selectStmt->whereClause->expr);
            conditionCols[find(tables[table->name].begin(),tables[table->name].end(),(string)(selectStmt->whereClause->expr2->expr->name))-tables[table->name].begin()].push_back(selectStmt->whereClause->expr2);
        }

        for(int i=0;i<attributes.size();i++) {
            vector<string>::iterator it;
            it=find(tables[table->name].begin(),tables[table->name].end(),attributes[i]->name);
            if(it==tables[table->name].end()) {
                cout<<"Column "<<attributes[i]->name<<" not found!\n";
                return true;
            }
            else {
                cols.push_back(it-tables[table->name].begin());
            }
        }

        ifstream tableFile((string)table->name+".csv");
        string line;

        for(int i=0;i<cols.size()-1;i++)
            cout<<table->name<<"."<<tables[table->name][cols[i]]<<",";
        cout<<table->name<<"."<<tables[table->name][cols[cols.size()-1]]<<endl;

        if(!selectStmt->selectDistinct) {
            while(!tableFile.eof()) {
                string output;

                tableFile>>line;
                vector<string> items=split(line,',');
                if(!checkConditions(conditionCols,items,fAnd,fOr, fOne)) continue;
                int i;
                for(i=0;i<cols.size()-1;i++) {
                    output+=items[cols[i]]+",";
                }
                output+=items[cols[i]]+"\n";
                cout<<output;

            }
        }
        else {
            map<string,bool> outputs;
            while(!tableFile.eof()) {
                string output;

                tableFile>>line;
                vector<string> items=split(line,',');
                if(!checkConditions(conditionCols,items,fAnd,fOr, fOne)) continue;

                int i;
                for(i=0;i<cols.size()-1;i++) {
                    output+=items[cols[i]]+",";
                }    		
                output+=items[cols[i]]+"\n";
                if(outputs.find(output)==outputs.end()) {
                    outputs[output]=true;
                    cout<<output;
                }
            }
        }
        return true;
    }
    return false;
}

void executeQueryTypeC2Util(string pref, int start, int end) {

    string curTable=attrGrpsVec[start].first;
    vector<hsql::Expr* > curCols=attrGrpsVec[start].second;

    ifstream tableFile(curTable+".csv");

    string line;

    string curPref;
    while(!tableFile.eof()) {
        curPref=pref;
        tableFile>>line;
        vector<string> items=split(line,',');
        // if(!checkConditions(conditionCols,items,fAnd,fOr, fOne)) continue;
        for(int i=0;i<curCols.size();i++) {
            curPref+=items[attrNum[curCols[i]]];
            if(end==start and i==curCols.size()-1) curPref+="\n";
            else curPref+=",";
        }
        if(start==end) cout<<curPref;
        if(start<end) executeQueryTypeC2Util(curPref, start+1,end);            
    }
}

bool executeQueryTypeC2() {

    if(table->type==hsql::kTableCrossProduct) {

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

        executeQueryTypeC2Util("", 0, attrGrpsVec.size()-1);
        return true;
    }
    return false;
}

bool executeQueryTypeF(hsql::TableRef* table, hsql::SelectStatement* selectStmt) {
    if(selectStmt->whereClause and
            selectStmt->whereClause->type==hsql::kExprOperator and
            (selectStmt->whereClause->op_type==hsql::Expr::SIMPLE_OP or
             selectStmt->whereClause->op_type==hsql::Expr::NOT_EQUALS or
             selectStmt->whereClause->op_type==hsql::Expr::LESS_EQ or
             selectStmt->whereClause->op_type==hsql::Expr::GREATER_EQ or
             selectStmt->whereClause->op_type==hsql::Expr::UMINUS) and
            table->type==hsql::kTableCrossProduct and
            (*table->list).size()==2 and
            selectStmt->whereClause->expr->type==hsql::kExprColumnRef and
            selectStmt->whereClause->expr2->type==hsql::kExprColumnRef) {


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

        //if(executeQueryTypeF()) continue;

        //if(executeQueryTypeA()) continue;

        //if(executeQueryTypeB()) continue;

        //if(executeQueryTypeC1DE()) continue;

        //if(executeQueryTypeC2()) continue;

    }    
    return 0;
}
