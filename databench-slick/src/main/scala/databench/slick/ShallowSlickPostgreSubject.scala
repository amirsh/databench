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
import javax.sql.DataSource
import databench.database.PostgreSqlDatabase
import Database.threadLocalSession
import com.jolbox.bonecp.BoneCPConfig
import com.jolbox.bonecp.BoneCP
import com.jolbox.bonecp.BoneCPDataSource
import scala.slick.yy.VirtualizedCG

class ShallowSlickPostgreSubject extends Bank[JInt] {
    // val pw = new java.io.PrintWriter("shallowlog"+scala.util.Random.nextInt())
    import scala.slick.yy.test.YYDefinitions._

    private val dataSource = {
        import PostgreSqlDatabase._
        PostgreSqlDatabase.loadDriver
        val config = new BoneCPConfig
        config.setJdbcUrl(url)
        config.setUsername(user)
        config.setPassword(password)
        config.setStatementsCacheSize(100)
        new BoneCPDataSource(config)
    }

    private val database =
        Database.forDataSource(dataSource)

    def tearDown =
        dataSource.close

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

    def warmUp {
        accountByIdExecutor(0)
        accountByIdTuple2Executor(0)
    }

    override def additionalVMParameters(forMultipleVMs: Boolean) = ""

    def transfer(from: JInt, to: JInt, value: Int) =
        withTransactionRetry {
            // val (fromAccount, toAccount) = accountsByIdsQuery(from, to).to[List].head
            val (fromAccount, toAccount) = accountsByIdsQuery(from, to)
            // pw.println(s"$fromAccount -> $toAccount")
            // pw.flush()
            def updateFromAccount = updateAccount(fromAccount, -value)
            def updateToAccount = updateAccount(toAccount, value)
            executeOrderedUpdatesToAvoidDeadlock(fromAccount, toAccount, updateFromAccount, updateToAccount)
        }

    private def updateAccount(account: Account, value: Int): Unit =
        updateAccount(
            account.id,
            account.balance + value,
            account.transfers + "," + value)

    private def updateAccount(id: Int, balance: Int, transfers: String) = {
        // import scala.slick.yy._
        // import Shallow._
        // pw.println(s"$id updating...")
        // pw.flush()
        // val r = shallow {
        //     Queryable[Account].filter(_.id == id).update(Account(id, balance, transfers))
        // }
        // pw.println(s"$id result for update: $r")
        // pw.flush()
        // shallow {
        //     Queryable[Account].filter(_.id == id).executor
        // }.update(Account(id, balance, transfers))
        // accountByIdExecutor(id).update(Account(id, balance, transfers))
        accountByIdTuple2Executor(id).update((balance, transfers))
        // import VirtualizedCG._
        // val query =
        //     for (account <- AccountTable if (account.id === id.bind))
        //         yield account
        // query.update(Account(id, balance, transfers))

    }

    def getAccountStatus(id: JInt) = 
        withTransactionRetry {
            val account = accountById(id)
            new AccountStatus(
                account.balance,
                account.transferValues.toArray)
        }

    private def accountByIdExecutor(id: Int) = {
        import scala.slick.yy._
        import Shallow._
        shallow {
            Queryable[Account].filter(_.id == id).executor
        }
    }

    private def accountByIdTuple2Executor(id: Int) = {
        import scala.slick.yy._
        import Shallow._
        shallow {
            Queryable[Account].filter(_.id == id).map(x => (x.balance, x.transfers)).executor
        }
    }

    private def accountById(id: Int) = {
        // import scala.slick.yy._
        // import Shallow._
        // shallow {
        //     Queryable[Account].filter(_.id == id).executor
        // }.first()
        accountByIdExecutor(id).first()
        // import VirtualizedCG._
        // val q = for (
        //     id <- Parameters[Int];
        //     account <- AccountTable if (account.id === id)
        // ) yield account
        // q(id).first
    }

        // accountByIdQuery(id).to[List].head

    // private val accountByIdQuery = {
    //     import VirtualizedCG._
    //     for (
    //         id <- Parameters[Int];
    //         account <- AccountTable if (account.id === id)
    //     ) yield account
    // }

    // private val accountsByIdsQuery = {
    //     import VirtualizedCG._
    //     (for (
    //         (from, to) <- Parameters[(Int, Int)];
    //         fromAccount <- AccountTable.where(_.id is from);
    //         toAccount <- AccountTable.where(_.id is to)
    //     ) yield (fromAccount, toAccount))
    // }

    private def accountsByIdsQuery(from: Int, to: Int) = {
        val fromAccount = accountById(from)
        val toAccount = accountById(to)
        (fromAccount, toAccount)
    }

    private def insertAccounts(numberOfAccounts: Integer) = {
        // import VirtualizedCG._
        // withTransaction {
        //     for (i <- (0 until numberOfAccounts)) yield {
        //         AccountTable.insert(Account(i, 0, " "))
        //         i
        //     }
        // }
        
        withTransaction {
            for (i <- (0 until numberOfAccounts)) yield {
                import scala.slick.yy._
                import Shallow._
                // pw.println(s"$i inserting...")
                // pw.flush()
                val r = shallow {
                    Queryable[Account].executor
                }.insert(Account(i, 0, " "))
                // pw.println(s"$i result for insert: $r")
                // pw.flush()
                i
            }
        }
    }

    private def createSchema = {
        import VirtualizedCG._
        withTransaction {
            AccountTable.ddl.create
        }
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

    private def withTransaction[R](f: => R): R =
        database.withTransaction(f)

}

