package scala.slick.yy
object VirtualizedCG extends scala.slick.driver.PostgresDriver.ImplicitJdbcTypes {
  import scala.slick.yy.test.YYDefinitions._
  import scala.language.implicitConversions
    
  type AccountRow = Account
  val AccountRow = Account
  class AccountTable extends _root_.scala.slick.driver.PostgresDriver.simple.Table[AccountRow] ("SLICK_ACCOUNT"){
    def id = AccountTable.this.column[scala.Int]("ID", O.PrimaryKey)
    def balance = AccountTable.this.column[scala.Int]("BALANCE")
    def transfers = AccountTable.this.column[java.lang.String]("TRANSFERS", O.DBType("VARCHAR"))
    def * = AccountTable.this.id ~ AccountTable.this.balance ~ AccountTable.this.transfers <> (AccountRow, x$0 => (AccountRow unapply x$0))
  }
  object AccountTable extends AccountTable (){

  }
  class YYAccountTable(val table: AccountTable) extends _root_.scala.slick.yy.YYTable[AccountRow] (){
    def id = _root_.scala.slick.yy.YYColumn apply YYAccountTable.this.table.id
    def balance = _root_.scala.slick.yy.YYColumn apply YYAccountTable.this.table.balance
    def transfers = _root_.scala.slick.yy.YYColumn apply YYAccountTable.this.table.transfers
  }
  implicit object implicitYYAccountTable extends YYAccountTable (AccountTable){

  }
  implicit def convImplicitYYAccountTable(x: _root_.scala.slick.yy.YYRep[AccountRow]): YYAccountTable = new YYAccountTable(x.underlying.asInstanceOf[AccountTable])
}