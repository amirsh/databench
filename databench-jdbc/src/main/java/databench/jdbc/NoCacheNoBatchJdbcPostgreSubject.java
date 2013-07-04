package databench.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.ConnectionHandle;

import databench.AccountStatus;
import databench.Bank;
import databench.database.PostgreSqlDatabase;

public class NoCacheNoBatchJdbcPostgreSubject extends NoCacheJdbcPostgreSubject {

	public NoCacheNoBatchJdbcPostgreSubject() throws SQLException {
	}

	protected NoCacheTransferWorker getNoCacheTransferWorker(ConnectionHandle handle) {
		Connection conn = handle.getInternalConnection();
		NoCacheTransferWorker worker = new NoCacheNoBatchTransferWorker(conn);
		return worker;
	}

}
