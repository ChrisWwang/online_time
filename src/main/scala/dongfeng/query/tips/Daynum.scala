package dongfeng.query.tips

object Daynum {

  var num: Int = _
  var month: Int = _

  def setNum(num: Int, month: Int): Unit = {
    this.num = num
    this.month = month
  }

  def getNum(): Int = {
    num
  }

  def getMonth(): Int = {
    month
  }

}
