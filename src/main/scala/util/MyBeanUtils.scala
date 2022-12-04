package util

import bean.{DauInfo, PageLog}

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * 将srcObj中属性的值拷贝到destObj对应的属性上
 */
object MyBeanUtils {
/*  def main(args: Array[String]): Unit = {
    val pageLog: PageLog =
      PageLog("mid1001", "uid101", "prov101", null, null, null, null, null, null, null, null, null, null, 0L, null, 123456)

    val dauInfo: DauInfo = new DauInfo()
    println("拷贝前: " + dauInfo)

    copyProperties(pageLog, dauInfo)

    println("拷贝后: " + dauInfo)
  }*/

  //AnyRef（所有引用类型的基类，除了值类型，所有类型都继承自AnyRef）AnyVal（AnyVal 所有值类型的基类，描述的是值，而不是代表一个对象）
  //Scala 中的 Any 类型是所有其他类的超类，Any 是 abstract 类，它是 Scala 类继承结构中最底层的。 所有运行环境中的 Scala 类都是直接或间接继承自 Any 这个类，它就是其它语言（.Net， Java 等）中的 Object

  def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {
    if (srcObj == null || destObj == null) {
      return
    }
    //JAVA反射机制是在运行状态中，对于任意一个类，都能够知道这个类的所有属性和方法；
    //对于任意一个对象，都能够调用它的任意一个方法和属性；动态获取的信息以及动态调用对象的方法

    //获取到srcObj中所有的属性   Field 对象表示具有公共数据类型和一组公共属性的数据列
    //getClass：获取class对象
    //getDeclaredFields：获得某个类的所有声明的字段，即包括public、private和proteced，但是不包括父类的申明字段
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    //处理每个属性的拷贝
    for (srcField <- srcFields) {
      Breaks.breakable {
        //Scala会自动为类中的属性提供get、set方法
        //get:fieldname()
        //set:fieldname_$eq(参数类型)

        //获取(getXxx()方法)名称
        var getMethodName: String = srcField.getName  //返回此 Field 对象表示的字段的名称
        //设置(setXxx()方法)名称
        var setMethodName: String = srcField.getName + "_$eq"

        //从srcObj中获取相应字段的get方法
        val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
        //从destObj中获取相应字段的set方法
        val setMethod: Method =
          try {
            destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)   //getType返回字段的声明类型
          } catch {
            //NoSuchMethodException
            /**
             * scala中没有break关键字，采用Breaks.break()跳出循环， 实现原理是抛出异常改变逻辑顺序。
             * 然而一旦抛出异常，逻辑无法继续执行，需要结合 Breaks.breakable()使用
             */
             //模式匹配（类型匹配）
            case ex: Exception => Breaks.break()
          }

        //忽略val属性
        val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
        if (destField.getModifiers.equals(Modifier.FINAL)) {
          Breaks.break()
        }
        //invoke : 调用某个类中的方法
        //调用get方法获取到srcObj属性的值，再调用set方法将获取到的属性赋值给destObj的属性
        setMethod.invoke(destObj, getMethod.invoke(srcObj))
      }
    }
  }

}
