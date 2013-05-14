// Define the number of rows to process in the batch 
var batch = 35000;

// Get the prepare table url of the Shield RESTful webapp
function getPrepareTableUrl(rsKey, tableName) {
	return "http://llbpal36.pal.sap.corp:8080/shs/rest/preparer/"+rsKey+"/"+tableName;
}

// Get the result looping url of the Shield RESTful webapp
function getResultLoopUrl(rsKey) {	
	return "http://llbpal36.pal.sap.corp:8080/shs/rest/result/" + rsKey + "/" + batch;
}

// Convert the lines in csv into an array 
function csvToArray(text) {
    var re_valid = /^\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*(?:,\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*)*$/;
    var re_value = /(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g;
    // Return NULL if input string is not well formed CSV string.
    if (!re_valid.test(text)) {
        return null;
    }
    var a = [];            // Initialize array to receive values.
    text.replace(re_value, // "Walk" the string using replace with callback.
        function(m0, m1, m2, m3) {
            if (m1 !== undefined) {
				// Remove backslash from \' in single quoted values.
				a.push(m1.replace(/\\'/g, "'"));
            } else if (m2 !== undefined) {
				// Remove backslash from \" in double quoted values.
				a.push(m2.replace(/\\"/g, '"'));
            } else if (m3 !== undefined) {
				a.push(m3);
			}
            return ''; // Return empty string.
        });
    // Handle special case of empty last value.
    if (/,\s*$/.test(text)) {
		a.push('');
    }
    return a;
}

// Fetch the content of the url via HTTP 
function getHttpContent(url) {
	var con = $.net.http.getConnection(url);
	var resp = con.request("GET", "");
	var text = resp.getBody(0);		
	con.close();
	return text;
}

function getResult(rsKey) {
	try {
		var url = getResultLoopUrl(rsKey, batch);
		// Get the result set csv from the url		
		var text = getHttpContent(url);		
		// Check the result set key since it is included as the error message 
		// in the last batch  
		if (text.indexOf(rsKey) > 0) {
			text = "";
		}		
		return text;
	} catch (e) {
		return "";		
	}	
}

function prepareTable(rsKey, tableName) {
	try {
		var url = getPrepareTableUrl(rsKey, tableName);
		// Trigger Shield to create the destination HANA table for data loading
		return getHttpContent(url);
	} catch (e) {
		return "";		
	}	
}

function getInsertSql(tableName, columnCount) {
	var sql = "INSERT INTO " + tableName + " VALUES( ";
	var i = 0;
	for (i = 0; i < columnCount; i++) {
		sql += " ?";
		if (i < (columnCount - 1)) {
			sql += ",";
		}
	}
	sql = sql + " )";
	return sql;
}

function insertBatch(pstmt, subCsvRows) {
	var i = 0;
	var j = 0;
	var rowValues;
	var lineCount;
//$.trace.debug('>>>>subCsvRows.length=' + subCsvRows.length);	
	if (subCsvRows.length > 1) {
//$.trace.debug('In insertBatch(), >1 subCsvRows.length=' + subCsvRows.length);
		if (subCsvRows[subCsvRows.length - 1] === "") {
			lineCount = subCsvRows.length - 1;			
		} else {
			lineCount = subCsvRows.length;			
		}
//$.trace.debug('AAA-1 lineCount=' + lineCount);		
		pstmt.setBatchSize(lineCount);
		for (i = 0; i < lineCount; i++) {
			rowValues = csvToArray(subCsvRows[i]);
			for (j = 0; j < rowValues.length; j++) {
				pstmt.setString((j + 1), rowValues[j]); 
			}
			pstmt.addBatch();
		}
//$.trace.debug('AAA-2 lineCount=' + lineCount);		
		pstmt.executeBatch();
//$.trace.debug('AAA-3 lineCount=' + lineCount);		
		return lineCount;	
	} else if (subCsvRows.length === 1) {
//$.trace.debug('In insertBatch(), 1 subCsvRows.length=' + subCsvRows.length);
		rowValues = csvToArray(subCsvRows[0]);
		pstmt.setString(1, rowValues[0]);
		pstmt.setString(2, rowValues[1]);
		pstmt.setString(3, rowValues[2]);
		pstmt.execute();
		return 1;
	} else {
//$.trace.debug('In insertBatch(), ELSE subCsvRows.length=' + subCsvRows.length);
		return 0;
	}
}

function doInsert(tableName, csvRows) {		
    try {
//$.session.login("SYSTEM", "Hana1234");		
//var loginOk = $.session.login("SYSTEM", "Hana1234");
//$.trace.debug('AAA $.session.login("SYSTEM", "Hana1234")=' + $.session.login("SYSTEM", "Hana1234"));		
//$.trace.debug('BBB loginOk=' + loginOk);		
		var conn = $.db.getConnection();		
		var sql = "";
		var pstmt;					
		sql = getInsertSql(tableName, csvToArray(csvRows[0]).length);
//$.trace.debug('sql=' + sql);		
		pstmt = conn.prepareStatement( sql );
//$.trace.debug('csvRows.length=' + csvRows.length);		
		var inserted = insertBatch(pstmt, csvRows);
		
		pstmt.close();
		conn.commit();
		conn.close();
		return inserted;
	} catch (error) {
//$.trace.debug('In doInsert(), error=' + error);
		return -1;
	}
}

function main() {
	$.response.contentType = "text/plain";
	var doPrepare = $.request.parameters.get("prepareTable");
	var rsKey = $.request.parameters.get("rsKey");
	var tableName = $.request.parameters.get("tableName");
	var batchShieldResult;
	var batchShieldResultArray;
	var insertedText = "";
	var inserted = 0;
	try {
	$.trace.debug('doPrepare=' + doPrepare);
		if ('true' === doPrepare) {
			prepareTable(rsKey, tableName);	
		}
		batchShieldResult = getResult(rsKey);
	$.trace.debug('batchShieldResult=' + batchShieldResult);	
		if (batchShieldResult !== "") {
			batchShieldResultArray = batchShieldResult.split('\n');
	$.trace.debug('batchShieldResultArray.length=' + batchShieldResultArray.length);		
			inserted = doInsert(tableName, batchShieldResultArray);
	$.trace.debug('.....inserted=' + inserted);		
		}
		if (inserted >= 0) {
	$.trace.debug('111 >> inserted=' + inserted);		
			$.response.setBody(inserted);	
			$.response.status = $.net.http.OK;  
		} else {
	$.trace.debug('222 >> inserted=' + inserted);		
			$.response.setBody("Error inserting rows into the table: " + tableName);
			$.response.status = $.net.http.OK;
		}
	} catch (error) {
		$.response.setBody(error);	
		$.response.status = $.net.http.OK;
	}
}

main();
