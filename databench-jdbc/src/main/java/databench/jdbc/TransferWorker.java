package databench.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class TransferWorker {

	private final PreparedStatement updateAccountPreparedStatement;

	public TransferWorker(Connection connection) {
		try {
			updateAccountPreparedStatement = connection
					.prepareStatement("UPDATE JDBCACCOUNT SET balance = balance + ?, transfers=(transfers || ',' || ?) WHERE id=?");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void transfer(int from, int to, int value) throws SQLException {
		performOrderedUpdatesToAvoidDeadLock(from, to, value);
		updateAccountPreparedStatement.executeBatch();
	}

	private void performOrderedUpdatesToAvoidDeadLock(int from, int to,
			int value) throws SQLException {
		if (from < to) {
			addUpdateBatch(from, -value);
			addUpdateBatch(to, value);
		} else {
			addUpdateBatch(to, value);
			addUpdateBatch(from, -value);
		}
	}

	private void addUpdateBatch(int id, int value) throws SQLException {
		updateAccountPreparedStatement.setInt(1, value);
		updateAccountPreparedStatement.setInt(2, value);
		updateAccountPreparedStatement.setInt(3, id);
		updateAccountPreparedStatement.addBatch();
	}

}

class NoCacheTransferWorker {

	protected final Connection connection;

	public NoCacheTransferWorker(Connection connection) {
		this.connection = connection;
	}

	public void transfer(int from, int to, int value) throws SQLException {
		PreparedStatement updateAccountPreparedStatement = connection
					.prepareStatement("UPDATE JDBCACCOUNT SET balance = balance + ?, transfers=(transfers || ',' || ?) WHERE id=?");
		performOrderedUpdatesToAvoidDeadLock(from, to, value, updateAccountPreparedStatement);
		updateAccountPreparedStatement.executeBatch();
	}

	protected void performOrderedUpdatesToAvoidDeadLock(int from, int to,
			int value, PreparedStatement updateAccountPreparedStatement) throws SQLException {
		if (from < to) {
			addUpdateBatch(from, -value, updateAccountPreparedStatement);
			addUpdateBatch(to, value, updateAccountPreparedStatement);
		} else {
			addUpdateBatch(to, value, updateAccountPreparedStatement);
			addUpdateBatch(from, -value, updateAccountPreparedStatement);
		}
	}

	protected void addUpdateBatch(int id, int value, PreparedStatement updateAccountPreparedStatement) throws SQLException {
		updateAccountPreparedStatement.setInt(1, value);
		updateAccountPreparedStatement.setInt(2, value);
		updateAccountPreparedStatement.setInt(3, id);
		updateAccountPreparedStatement.addBatch();
	}

}

class NoCacheNoBatchTransferWorker extends NoCacheTransferWorker {
	public NoCacheNoBatchTransferWorker(Connection connection) {
		super(connection);
	}

    public void transfer(int from, int to, int value) throws SQLException {
		PreparedStatement updateAccountPreparedStatement = connection
					.prepareStatement("UPDATE JDBCACCOUNT SET balance = balance + ?, transfers=(transfers || ',' || ?) WHERE id=?");
		performOrderedUpdatesToAvoidDeadLock(from, to, value, updateAccountPreparedStatement);
	}

	protected void addUpdateBatch(int id, int value, PreparedStatement updateAccountPreparedStatement) throws SQLException {
		updateAccountPreparedStatement.setInt(1, value);
		updateAccountPreparedStatement.setInt(2, value);
		updateAccountPreparedStatement.setInt(3, id);
		updateAccountPreparedStatement.execute();
	}

}