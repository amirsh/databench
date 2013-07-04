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

public class NoCacheJdbcPostgreSubject extends JdbcPostgreSubject {

	public NoCacheJdbcPostgreSubject() throws SQLException {
	}

	@Override
	public AccountStatus getAccountStatus(Integer id) {
		ConnectionHandle conn = getConnection();
		try {
			NoCacheGetAccountStatusWorker worker = getNoCacheGetAccountStatusWorker(conn);
			return worker.getAccountStatus(id);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			releaseConnection(conn);
		}
	}

	@Override
	public void transfer(Integer from, Integer to, int amount) {
		ConnectionHandle conn = getConnection();
		try {
			NoCacheTransferWorker worker = getNoCacheTransferWorker(conn);
			worker.transfer(from, to, amount);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			releaseConnection(conn);
		}
	}

	protected BoneCPConfig getBoneCPConfig() {
		BoneCPConfig config = new BoneCPConfig();
		config.setJdbcUrl(PostgreSqlDatabase.url());
		config.setUsername(PostgreSqlDatabase.user());
		config.setPassword(PostgreSqlDatabase.password());
		config.setStatementsCacheSize(100);
		config.setDefaultAutoCommit(true);
		return config;
	}

	private NoCacheGetAccountStatusWorker getNoCacheGetAccountStatusWorker(
			ConnectionHandle handle) {
		Connection conn = handle.getInternalConnection();
		NoCacheGetAccountStatusWorker worker = new NoCacheGetAccountStatusWorker(conn);
		return worker;
	}

	protected NoCacheTransferWorker getNoCacheTransferWorker(ConnectionHandle handle) {
		Connection conn = handle.getInternalConnection();
		NoCacheTransferWorker worker = new NoCacheTransferWorker(conn);
		return worker;
	}

}
