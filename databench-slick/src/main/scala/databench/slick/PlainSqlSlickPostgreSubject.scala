package databench.slick

import databench.Bank
import java.lang.{ Integer => JInt }
import databench.AccountStatus
import java.sql.Connection
import scala.annotation.tailrec
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.slick.driver.ExtendedDriver
import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import Q.interpolation
import javax.sql.DataSource
import databench.database.PostgreSqlDatabase
import Database.threadLocalSession
import com.jolbox.bonecp.BoneCPConfig
import com.jolbox.bonecp.BoneCP
import com.jolbox.bonecp.BoneCPDataSource

// class AutoPlainSQlSlickPostgreSubject extends PlainSqlSlickPostgreSubject {
//   override def getBoneCPConfig = {
//     val config = super.getBoneCPConfig
//     config.setDefaultAutoCommit(true)
//     config
//   }
// }

// class CachedPlainSQlSlickPostgreSubject extends PlainSqlSlickPostgreSubject {
//   override def getBoneCPConfig = {
//     val config = super.getBoneCPConfig
//     config.setStatementsCacheSize(100)
//     config
//   }
// }

// class AutoCachedPlainSQlSlickPostgreSubject extends PlainSqlSlickPostgreSubject {
//   override def getBoneCPConfig = {
//     val config = super.getBoneCPConfig
//     config.setDefaultAutoCommit(true)
//     config.setStatementsCacheSize(100)
//     config
//   }
// }

class PlainSqlSlickPostgreSubject extends Bank[JInt] {
    import LiftedDefs._

    implicit val getAccountResult = GetResult(c => new Account(c.<<, c.<<, c.<<))

    private val dataSource = {
        
        PostgreSqlDatabase.loadDriver
        val config = getBoneCPConfig
        // config.setDefaultAutoCommit(true)
        new BoneCPDataSource(config)
    }

    def getBoneCPConfig = {
        import PostgreSqlDatabase._
        val config = new BoneCPConfig
        config.setJdbcUrl(url)
        config.setUsername(user)
        config.setPassword(password)
        config.setStatementsCacheSize(100)
        config
    }

    private val database =
        Database.forDataSource(dataSource)

    def tearDown =
        dataSource.close

    // @tailrec private def withTransactionRetry[R](f: Session => R): R =
    @tailrec private def withTransactionRetry[R](f: => R): R =
        Try(withTransaction(f)) match {
            case Success(result) =>
                result
            case Failure(exception) if (
                exception.getMessage.startsWith("ERROR: could not serialize access")) =>
                withTransactionRetry(f)
        }

    def setUp(numberOfAccounts: JInt): Array[JInt] = {
        PostgreSqlDatabase.setDatabaseDefaultTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
        createSchema
        insertAccounts(numberOfAccounts).map(new JInt(_)).toArray
    }

    def warmUp = {}

    override def additionalVMParameters(forMultipleVMs: Boolean) = ""

    def transfer(from: JInt, to: JInt, value: Int) =
        // withTransactionRetry { implicit Session =>
        withTransactionRetry {
            val (fromAccount, toAccount) = accountsByIdsQuery(from, to)
            def updateFromAccount = updateAccount(fromAccount, -value)
            def updateToAccount = updateAccount(toAccount, value)
            executeOrderedUpdatesToAvoidDeadlock(fromAccount, toAccount, updateFromAccount, updateToAccount)
        }

    // private def updateAccount(account: Account, value: Int)(implicit session: Session) {
    private def updateAccount(account: Account, value: Int) {
        updateAccount(
            account.id,
            account.balance + value,
            account.transfers + "," + value)
        // sqlu"UPDATE SLICK_PLAIN_ACCOUNT SET BALANCE = ${account.balance} + $value, TRANSFERS=(${account.transfers} || ',' || $value) WHERE ID=${account.id}".first()
    }

    // private def updateAccount(id: Int, balance: Int, transfers: String)(implicit session: Session) = {
    private def updateAccount(id: Int, balance: Int, transfers: String) = {
        sqlu"UPDATE SLICK_PLAIN_ACCOUNT SET BALANCE = $balance, TRANSFERS=$transfers WHERE ID=$id".first()

    }

    def getAccountStatus(id: JInt) =
        withTransactionRetry {
        // withTransactionRetry { implicit Session =>
            val account = accountById(id)
            new AccountStatus(
                account.balance,
                account.transferValues.toArray)
        }

    // private def accountById(id: Int)(implicit session: Session) =
    private def accountById(id: Int) =
        sql"select id, balance, transfers from SLICK_PLAIN_ACCOUNT where id = $id".as[Account].first()

    // private def accountsByIdsQuery(from: Int, to: Int)(implicit session: Session) = {
    private def accountsByIdsQuery(from: Int, to: Int) = {
        val fromAccount = accountById(from)
        val toAccount = accountById(to)
        (fromAccount, toAccount)
    }

    private def insertAccounts(numberOfAccounts: Integer) =
        // withTransaction { implicit Session =>
        withTransaction {
            for (i <- (0 until numberOfAccounts)) yield {
                sqlu"INSERT INTO SLICK_PLAIN_ACCOUNT VALUES ($i, 0, '')".execute()
                i
            }
        }

    private def createSchema =
        // withTransaction { implicit Session =>
        withTransaction {
            sqlu"CREATE TABLE IF NOT EXISTS SLICK_PLAIN_ACCOUNT (ID INTEGER PRIMARY KEY, BALANCE INTEGER, TRANSFERS VARCHAR )".execute()
            // Accounts.ddl.create
        }

    private def executeOrderedUpdatesToAvoidDeadlock(
        fromAccount: Account,
        toAccount: Account,
        updateFromAccount: => Unit,
        updateToAccount: => Unit) =
        if (fromAccount.id < toAccount.id) {
            updateFromAccount
            updateToAccount
        } else {
            updateToAccount
            updateFromAccount
        }

    private def withTransaction[R](f: => R): R = {
    // private def withTransaction[R](f: Session => R): R = {
        database.withTransaction(f)
        // database.withSession(f) 
        // database.withTransactionOptimized(f)
        // database.createSession.withTransactionOptimized(f)
    }

}

