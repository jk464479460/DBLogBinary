using System;
using System.Data;

namespace ConsoleApplication7
{
    //自定义的column结构
    public class Datacolumn
    {
        public string Name;
        public System.Data.SqlDbType DataType;
        public short Length = -1;
        public object Value = null;
        public Datacolumn(string name, System.Data.SqlDbType type)
        {
            Name = name;
            DataType = type;
        }
        public Datacolumn(string name, System.Data.SqlDbType type, short length)
        {
            Name = name;
            DataType = type;
            Length = length;
        }
    }
    class Program
    {
        static void TranslateData(byte[] data, Datacolumn[] columns)
        {
            //我只根据示例写了Char,DateTime,Int三种定长度字段和varchar一种不定长字段，其余的有兴趣可以自己补充
            //这里没有暂时没有考虑Null和空字符串两种情况，以后会补充。
            //引用请保留以下信息：
            //作者：jinjazz 
            //sql的数据行二进制结构参考我的blog
            //http://blog.csdn.net/jinjazz/archive/2008/08/07/2783872.aspx
            //行数据从第5个字节开始
            short index = 4;
            //先取定长字段
            foreach (Datacolumn c in columns)
            {
                switch (c.DataType)
                {
                    case System.Data.SqlDbType.Char:
                        //读取定长字符串，需要根据表结构指定长度
                        c.Value = System.Text.Encoding.Default.GetString(data, index, c.Length);
                        index += c.Length;
                        break;
                    case System.Data.SqlDbType.DateTime:
                        //读取datetime字段，sql为8字节保存
                        System.DateTime date = new DateTime(1900, 1, 1);
                        //前四位1/300秒保存
                        int second = BitConverter.ToInt32(data, index);
                        date = date.AddSeconds(second / 300);
                        index += 4;
                        //后四位1900-1-1的天数
                        int days = BitConverter.ToInt32(data, index);
                        date = date.AddDays(days);
                        index += 4;
                        c.Value = date;
                        break;
                    case System.Data.SqlDbType.Int:
                        //读取int字段,为4个字节保存
                        c.Value = BitConverter.ToInt32(data, index);
                        index += 4;
                        break;
                    default:
                        //忽略不定长字段和其他不支持以及不愿意考虑的字段
                        break;
                }
            }
            //跳过三个字节
            index += 3;
            //取变长字段的数量,保存两个字节
            short varColumnCount = BitConverter.ToInt16(data, index);
            index += 2;
            //接下来,每两个字节保存一个变长字段的结束位置,
            //所以第一个变长字段的开始位置可以算出来
            short startIndex = (short)(index + varColumnCount * 2);
            //第一个变长字段的结束位置也可以算出来
            short endIndex = BitConverter.ToInt16(data, index);
            //循环变长字段列表读取数据
            foreach (Datacolumn c in columns)
            {
                switch (c.DataType)
                {
                    case System.Data.SqlDbType.VarChar:
                        //根据开始和结束位置，可以算出来每个变长字段的值
                        c.Value = System.Text.Encoding.Default.GetString(data, startIndex, endIndex - startIndex);
                        //下一个变长字段的开始位置
                        startIndex = endIndex;
                        //获取下一个变长字段的结束位置
                        index += 2;
                        endIndex = BitConverter.ToInt16(data, index);
                        break;
                    default:
                        //忽略定长字段和其他不支持以及不愿意考虑的字段
                        break;
                }
            }
            //获取完毕
        }
        static void Main(string[] args)
        {
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection())
            {
                conn.ConnectionString = "server=.;uid=sa;pwd=iamnts;database=Test";
                conn.Open();

                using (System.Data.SqlClient.SqlCommand command = conn.CreateCommand())
                {
                    //察看dbo.log_test对象的sql日志
                    command.CommandText = @"SELECT allocunitname,operation,[RowLog Contents 0] as r0,[RowLog Contents 1]as r1 
                                from::fn_dblog (null, null)   
                                where allocunitname like 'dbo.log_test%'and
                                operation in('LOP_INSERT_ROWS','LOP_DELETE_ROWS')";
                    System.Data.SqlClient.SqlDataReader reader = command.ExecuteReader();
                    Datacolumn[] columns = new Datacolumn[]
                       {
                            new Datacolumn("id", System.Data.SqlDbType.Int),
                            new Datacolumn("code", System.Data.SqlDbType.Char,10),
                            new Datacolumn("name", System.Data.SqlDbType.VarChar),
                            new Datacolumn("date", System.Data.SqlDbType.DateTime),
                            new Datacolumn("memo", System.Data.SqlDbType.VarChar)
                       };
                    while (reader.Read())
                    {
                        byte[] data = (byte[])reader["r0"];

                        try
                        {
                            //把二进制数据结构转换为明文
                            TranslateData(data, columns);
                            Console.WriteLine("数据对象{1}的{0}操作：", reader["operation"], reader["allocunitname"]);
                            foreach (Datacolumn c in columns)
                            {
                                Console.WriteLine("{0} = {1}", c.Name, c.Value);
                            }
                            Console.WriteLine();
                        }
                        catch
                        {
                            //to-do...
                        }

                    }
                    reader.Close();
                }
            }
        }
    }
}
