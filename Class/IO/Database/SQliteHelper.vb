Imports System.Data.Common
Imports System.Data.SQLite
Imports System.Text
Imports System.Text.RegularExpressions
Imports System.Threading.Tasks
Imports System.Windows.Forms
Imports ShanXingTech.Exception2
Imports ShanXingTech.Text2

Namespace ShanXingTech.IO2.Database
    Public Class SQLiteHelper
#Region "常量区"

#End Region

#Region "属性区"
        Private Shared s_DBFileFullPath As String
        ''' <summary>
        ''' 数据库文件路径
        ''' </summary>
        ''' <returns></returns>
        Public Shared ReadOnly Property DBFileFullPath() As String
            Get
                Return s_DBFileFullPath
            End Get
        End Property

        Private Shared s_Pooling As Boolean
        Public Shared Property Pooling() As Boolean
            Get
                Return s_Pooling
            End Get
            Set(ByVal value As Boolean)
                s_Pooling = value
            End Set
        End Property

        Private Shared s_FailIfMissing As Boolean
        Public Shared Property FailIfMissing() As Boolean
            Get
                Return s_FailIfMissing
            End Get
            Set(ByVal value As Boolean)
                s_FailIfMissing = value
            End Set
        End Property

        Private Shared s_JournalMode As Boolean
        Public Shared Property JournalMode() As Boolean
            Get
                Return s_JournalMode
            End Get
            Set(ByVal value As Boolean)
                s_JournalMode = value
            End Set
        End Property

        Private Shared Property connectionString As String
#End Region

#Region "字段区"
        Private Shared ReadOnly sqliteUnInitializeMessage As String
        Private Shared connectionStringTemplete As String

#End Region

#Region "构造函数区"
        ''' <summary>
        ''' 类构造函数
        ''' 类之内的任意一个静态方法第一次调用时调用此构造函数
        ''' 而且程序生命周期内仅调用一次
        ''' </summary>
        Shared Sub New()
            s_Pooling = True
            s_JournalMode = True
            sqliteUnInitializeMessage = "尚未调用 Init 函数进行初始化"
            connectionStringTemplete = "Data Source={0};Pooling={1};FailIfMissing={2}"

            Dim expRst = ExportSQLiteDll()

            If Not expRst Then
                Throw New SQLiteInitializationException("导出SQLite dll文件失败")
            End If
        End Sub
#End Region

#Region "初始化相关"
        ''' <summary>
        ''' 根据系统位数导出相应的SQLite dll
        ''' </summary>
        ''' <returns></returns>
        Private Shared Function ExportSQLiteDll() As Boolean
            Dim funcRst As Boolean

            ' ################
            ' 有时间可以把 dll 以zip压缩包的形式添加到资源， 然后再用 
            'ShanXingTech.IO2.Compression.Zip 类提取出来 20180629 
            ' ################

            ' 根据系统位数创建相应的文件夹
            Dim startupPath = Application.StartupPath
            If Not startupPath.EndsWith("\") Then
                startupPath += "\"
            End If

            Dim folder = startupPath & "x86\"

            Dim resourcesName = NameOf(My.Resources.SQLite_Interop_x86)
            Dim resourcesMd5 = My.Resources.SQLiteInteropDll_x86_Md5
            ' 调用程序的编译选项处如果选择了AnyCPU并且勾选了首选32位，那么编译出来的程序 Is64BitProcess = False
            If Environment.Is64BitProcess Then
                folder = startupPath & "x64\"
                resourcesName = NameOf(My.Resources.SQLite_Interop_x64)
                resourcesMd5 = My.Resources.SQLiteInteropDll_x64_Md5
            End If
            Dim fileFullPath = folder & "SQLite.Interop.dll"

            Try
                ' 创建文件夹
                Dim isCreateSuccessed = IO2.Directory.Create(folder)
                If Not isCreateSuccessed Then
                    Return False
                End If

                ' 如果资源已经存在，并且md5值跟内置的一致，那么就不需要再释放
                Dim isExistsFile = IO.File.Exists(fileFullPath)
                If isExistsFile Then
                    Dim fileMd5 = IO2.File.GetMD5Value(fileFullPath)
                    ' 如果md5不一致表示已经被别人修改过，直接返回False
                    Return resourcesMd5 = fileMd5
                End If

                ' 释放资源文件
                Dim bufferSize = 80 * 1024
                Using fileStream As New IO.FileStream(fileFullPath,
                                                      System.IO.FileMode.Create,
                                                      System.IO.FileAccess.ReadWrite,
                                                      System.IO.FileShare.ReadWrite,
                                                      bufferSize,
                                                      True)
                    Dim resByte() As Byte = DirectCast(My.Resources.ResourceManager.GetObject(resourcesName）, Byte())
                    If resByte Is Nothing Then
                        Throw New DllNotFoundException(String.Format(My.Resources.FileNotFound, fileFullPath))
                    End If

                    fileStream.Write(resByte, 0, resByte.Length)
                End Using

                funcRst = True

                Debug.Print(Logger.MakeDebugString($"解压{resourcesName}成功"))
            Catch ex As Exception
                Throw
            Finally
                ' 如果创建文件失败，那么就删除（0字节文件）
                If IO.File.Exists(fileFullPath) AndAlso New IO.FileInfo(fileFullPath).Length = 0 Then
                    IO.File.Delete(fileFullPath)
                End If

                ' 如果文件夹为空，那么也删除
                If IO.Directory.Exists(folder) AndAlso
                    Win32API.PathIsDirectoryEmptyA(folder) Then
                    IO2.Directory.Delete(folder)
                End If
            End Try

            Return funcRst
        End Function

        ''' <summary>
        ''' 用于初始化类，如果调用不带 <paramref name="dbFileFullPath"/> 参数的函数，必须先调用此函数初始化;
        ''' pooling 默认为 True,failIfMissing 默认为 False, JournalMode 默认为 True
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        Public Shared Sub Init(ByVal dbFileFullPath As String)
            Init(dbFileFullPath, True, False)
        End Sub

        ''' <summary>
        ''' 用于初始化类，如果调用不带 <paramref name="dbFileFullPath"/> 参数的函数，必须先调用此函数初始化;
        ''' failIfMissing 默认为 False, JournalMode 默认为 True
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="pooling"></param>
        Public Shared Sub Init(ByVal dbFileFullPath As String, ByVal pooling As Boolean)
            Init(dbFileFullPath, pooling, False, True)
        End Sub

        ''' <summary>
        ''' 用于初始化类，如果调用不带 <paramref name="dbFileFullPath"/> 参数的函数，必须先调用此函数初始化;
        ''' JournalMode 默认为 True
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="pooling">默认为 True</param>
        ''' <param name="failIfMissing">默认为 False</param>
        Public Shared Sub Init(ByVal dbFileFullPath As String, ByVal pooling As Boolean, ByVal failIfMissing As Boolean)
            Init(dbFileFullPath, pooling, failIfMissing, True)
        End Sub

        ''' <summary>
        ''' 用于初始化类，如果调用不带 <paramref name="dbFileFullPath"/> 参数的函数，必须先调用此函数初始化
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="pooling">默认为 True</param>
        ''' <param name="failIfMissing">默认为 False</param>
        ''' <param name="journalMode">默认为 True</param>
        Public Shared Sub Init(ByVal dbFileFullPath As String, ByVal pooling As Boolean, ByVal failIfMissing As Boolean, ByVal journalMode As Boolean)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                s_DBFileFullPath = String.Empty
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            s_DBFileFullPath = dbFileFullPath
            s_Pooling = pooling
            s_FailIfMissing = failIfMissing
            s_JournalMode = journalMode

            connectionString = String.Format(connectionStringTemplete, s_DBFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString, s_JournalMode.ToString)
        End Sub
#End Region

#Region "异步函数区"
        ''' <summary>
        ''' SQLite建表
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql">建表语句</param>
        ''' <returns>成功 Success 返回True,失败 Success返回False,具体消息看 Message 参数</returns>
        Public Shared Async Function CreateTableAsync(ByVal dbFileFullPath As String, ByVal sql As String) As Task(Of (Success As Boolean, Message As String))
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim funcRst As Boolean
            Dim message = String.Empty

            ' 创建DB数据库路径
            ' 如果该目录已存在，则不引发任何异常。
            IO2.Directory.Create(dbFileFullPath)

            Try
                Using sqliteCn As New SQLiteConnection
                    sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                    If sqliteCn.State = ConnectionState.Closed Then
                        sqliteCn.Open()
                    End If

                    Dim sqliteCmd As New SQLiteCommand With {
                        .CommandText = sql,
                        .Connection = sqliteCn
                        }

                    Await sqliteCmd.ExecuteNonQueryAsync()
                End Using

                funcRst = True
                Debug.Print(Logger.MakeDebugString("建表过程完成"))

            Catch ex As AggregateException
                For Each innerEx As Exception In ex.InnerExceptions
                    Logger.WriteLine(innerEx)
                Next

                message = ex.Message
            End Try

            Return (funcRst, message)
        End Function

        ''' <summary>
        ''' SQLite建表
        ''' </summary>
        ''' <param name="sql">建表语句</param>
        ''' <returns>成功 Success 返回True,失败 Success返回False,具体消息看 Message 参数</returns>
        Public Shared Async Function CreateTableAsync(ByVal sql As String) As Task(Of (Success As Boolean, Message As String))
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await CreateTableAsync(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <param name="noUse">不使用，可以传入任何值</param>
        ''' <returns>成功——返回受影响行数,失败——返回-1</returns>
        Public Overloads Shared Async Function ExecuteNonQueryAsync(ByVal sql As String, ByVal noUse As Integer) As Task(Of Integer)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await ExecuteNonQueryAsync(s_DBFileFullPath, sql, noUse)
        End Function


        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <param name="noUse">不使用，可以传入任何值</param>
        ''' <returns>成功——返回受影响行数,失败——返回-1</returns>
        Public Overloads Shared Async Function ExecuteNonQueryAsync(ByVal dbFileFullPath As String, ByVal sql As String, ByVal noUse As Integer) As Task(Of Integer)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim refectRow As Integer

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                Dim sqliteCmd As New SQLiteCommand(sql, sqliteCn)

                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                refectRow = Await sqliteCmd.ExecuteNonQueryAsync()
            End Using

            Return refectRow
        End Function

        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作（批量事务操作）
        ''' 操作结果存储在返回的 TransactionReport 中
        ''' </summary>
        ''' <param name="sqls">需要执行的SQL语句表</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function ExecuteNonQueryAsync(ByVal sqls As List(Of String)) As Task(Of (TransactionReport As StringBuilder, SuccessCount As Integer))
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await ExecuteNonQueryAsync(s_DBFileFullPath, sqls)
        End Function

        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作（批量事务操作）
        ''' 操作结果存储在返回的 TransactionReport 中
        ''' </summary>
        ''' <param name="dbFileFullPath">数据库</param>
        ''' <param name="sqls">需要执行的SQL语句表</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function ExecuteNonQueryAsync(ByVal dbFileFullPath As String, ByVal sqls As List(Of String)) As Task(Of (TransactionReport As StringBuilder, SuccessCount As Integer))
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New SQLiteUnInitializeException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim rstSb = StringBuilderCache.Acquire(360)

            ' 添加成功的个数
            Dim successCount As Integer

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)

                Using sqliteCmd As New SQLiteCommand
                    If sqliteCn.State = ConnectionState.Closed Then
                        sqliteCn.Open()
                    End If

                    ' 开始事务
                    Dim trans As SQLiteTransaction = sqliteCn.BeginTransaction()
                    sqliteCmd.Transaction = trans

                    ' 无需再次赋值Connection属性
                    'sqliteCmd.Connection = sqliteCn

                    For Each sql As String In sqls
                        Try
                            sqliteCmd.CommandText = sql
                            Dim rst = Await sqliteCmd.ExecuteNonQueryAsync
                            If rst = 1 Then
                                successCount += 1
                            End If
                        Catch ex As SQLite.SQLiteException
                            ' 在数据库中已有此数据
                            If ex.ResultCode = SQLiteErrorCode.Constraint Then
                                rstSb.Append("ErrorItem:").Append(sql.PadRightByByte(20)).AppendLine("ErrorInfo:Duplicate")
                            Else
                                rstSb.Append("ErrorItem:").Append(sql.PadRightByByte(20)).AppendLine($"ErrorInfo:{ex.Message}")
                            End If
                            Continue For
                        Catch ex As Exception
                            '' 回滚事务 取消操作
                            'trans.Rollback()

                            Logger.WriteLine(ex)

                            Continue For
                        End Try
                    Next

                    ' 提交事务
                    trans.Commit()
                End Using
            End Using

            ' 总结添加结果
            Dim reportDetail = $"——————————————————————————————————————————————————{Environment.NewLine}"
            rstSb.Insert(0, reportDetail)
            reportDetail = $"Transaction Detail{Environment.NewLine}"
            rstSb.Insert(0, reportDetail)
            reportDetail = $"Total:{sqls.Count.ToStringOfCulture},success:{successCount.ToStringOfCulture}{Environment.NewLine}"
            rstSb.Insert(0, reportDetail)
            reportDetail = $"Transaction Report{Environment.NewLine}"
            rstSb.Insert(0, reportDetail)

            Return (rstSb, successCount)
        End Function

        ''' <summary>
        ''' 按表名和字段名获取相关数据
        ''' 只获取一个字段数据
        ''' </summary>
        ''' <param name="tableName">表名</param>
        ''' <param name="fieldName">字段名</param>
        ''' <param name="condition">查询条件</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataAsync(ByVal tableName As String, ByVal fieldName As String， ByVal condition As String) As Task(Of List(Of String))
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await GetDataAsync(s_DBFileFullPath, tableName, fieldName, condition)
        End Function

        ''' <summary>
        ''' 按表名和字段名获取相关数据
        ''' 只获取一个字段数据
        ''' </summary>
        ''' <param name="dbFileFullPath">数据库名</param>
        ''' <param name="tableName">表名</param>
        ''' <param name="fieldName">字段名</param>
        ''' <param name="condition">查询条件。如果不需要查询则传入nothing或者string.empty</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataAsync(ByVal dbFileFullPath As String, ByVal tableName As String, ByVal fieldName As String， Optional ByVal condition As String = Nothing) As Task(Of List(Of String))
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim result As New List(Of String)

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Dim sqliteCmd As New SQLiteCommand With {
                    .CommandText = $"select {fieldName} from {tableName} {If(condition.IsNullOrEmpty, String.Empty, String.Concat("where ", condition))}",
                    .Connection = sqliteCn
                }

                Using reader = Await sqliteCmd.ExecuteReaderAsync(CommandBehavior.CloseConnection)
                    While Await reader.ReadAsync()
                        result.Add(CStr(reader(0)))
                    End While
                End Using
            End Using

            Return result
        End Function

        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataAsync(ByVal sql As String) As Task(Of DataGridView)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await GetDataAsync(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataAsync(ByVal dbFileFullPath As String, ByVal sql As String) As Task(Of DataGridView)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim dgv As New DataGridView

            Debug.Print(Logger.MakeDebugString(sql))

            Try
                Using sqliteCn As New SQLiteConnection
                    sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                    If sqliteCn.State = ConnectionState.Closed Then
                        sqliteCn.Open()
                    End If

                    Dim sqliteCmd As New SQLiteCommand With {
                            .CommandText = sql,
                            .Connection = sqliteCn
                        }

                    Dim dt As DataTable = Await DataReaderAsync(sqliteCmd)

                    If dgv.InvokeRequired Then
                        dgv.Invoke(Sub() dgv.DataSource = dt)
                    Else
                        dgv.DataSource = dt
                    End If

                    If dgv.RowCount = 1 AndAlso dgv.Rows(0).Cells(1).Value Is Nothing Then
                        dgv.Rows(0).Selected = True
                    End If
                End Using
            Catch ex As AggregateException
                For Each innerex As Exception In ex.InnerExceptions
                    Logger.WriteLine(ex)
                Next
            End Try

            Return dgv
        End Function

        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <param name="showRowNumber">要在第一列显示的自动行数</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataTableAsync(ByVal sql As String, ByVal showRowNumber As Boolean) As Task(Of DataTable)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await GetDataTableAsync(s_DBFileFullPath, sql, showRowNumber)
        End Function

        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <param name="showRowNumber">要在第一列显示的自动行数</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataTableAsync(ByVal sql As String, ByVal showRowNumber As Boolean, ByVal rowNumberColumnName As String) As Task(Of DataTable)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await GetDataTableAsync(s_DBFileFullPath, sql, showRowNumber， rowNumberColumnName)
        End Function


        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <param name="showRowNumber">要在第一列显示的自动行数</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataTableAsync(ByVal dbFileFullPath As String, ByVal sql As String, ByVal showRowNumber As Boolean) As Task(Of DataTable)
            Return Await GetDataTableAsync(dbFileFullPath, sql, showRowNumber, "No.")
        End Function

        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <param name="showRowNumber">要在第一列显示的自动行数</param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataTableAsync(ByVal dbFileFullPath As String, ByVal sql As String, ByVal showRowNumber As Boolean, ByVal rowNumberColumnName As String) As Task(Of DataTable)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim rstDataTable As DataTable

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Dim sqliteCmd As New SQLiteCommand With {
                    .CommandText = sql,
                    .Connection = sqliteCn
                }

                Dim dt As DataTable = Await DataReaderAsync(sqliteCmd)

                '显示自动序号
                If showRowNumber Then
                    rstDataTable = New DataTable()
                    ' AutoIncrement  获取或设置一个值，该值指示对于添加到该表中的新行，列是否将列的值自动递增  
                    Dim column As New DataColumn() With {
                        .AutoIncrement = True,
                        .ColumnName = rowNumberColumnName,
                        .AutoIncrementSeed = 1,
                        .AutoIncrementStep = 1
                    }
                    rstDataTable.Columns.Add(column)
                    ' Merge合并DataTable  
                    ' table.Merge(dataTable)
                    rstDataTable.Merge(dt)
                Else
                    rstDataTable = dt
                End If
            End Using

            Return rstDataTable
        End Function

        ''' <summary>
        ''' 按表名和字段名获取相关数据
        ''' </summary>
        ''' <param name="tableName">表名</param>
        ''' <param name="fieldName">字段名</param>
        ''' <param name="condition">查询条件</param>
        Public Overloads Shared Async Function GetDataTableAsync(ByVal tableName As String, ByVal fieldName As String， ByVal condition As String) As Task(Of DataTable)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await GetDataTableAsync(s_DBFileFullPath, tableName, fieldName, condition)
        End Function


        ''' <summary>
        ''' 按表名和字段名获取相关数据
        ''' </summary>
        ''' <param name="dbFileFullPath">数据库名</param>
        ''' <param name="tableName">表名</param>
        ''' <param name="fieldName">字段名</param>
        ''' <param name="condition">查询条件</param>
        Public Overloads Shared Async Function GetDataTableAsync(ByVal dbFileFullPath As String, ByVal tableName As String, ByVal fieldName As String， ByVal condition As String) As Task(Of DataTable)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim dt As DataTable

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Dim sqliteCmd As New SQLiteCommand With {
                .CommandText = $"select {fieldName} from {tableName} {condition}",
                .Connection = sqliteCn
            }

                dt = Await DataReaderAsync(sqliteCmd)
            End Using

            Return dt
        End Function

        ''' <summary>
        ''' 返回查询的DataReader结果，建议使用using语句调用,以确保使用完之后关闭reader,并且要在用完Reader之后 End Using 语句之前调用 Reader的CloseDbConnection扩展方法,确保相应的数据库连接在关闭
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataReaderAsync(ByVal dbFileFullPath As String, ByVal sql As String) As Task(Of DbDataReader)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            ' 为了减少调用者的外部dll依赖，所以 返回的是 DbDataReader 而不是 SQLiteDataReader
            Dim reader As DbDataReader = Nothing

            Dim sqliteCn As New SQLiteConnection With {
                    .connectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                }

            Try
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Using sqliteCmd = New SQLiteCommand With {
                    .CommandText = sql,
                    .Connection = sqliteCn
                }

                    reader = Await sqliteCmd.ExecuteReaderAsync(CommandBehavior.CloseConnection)
                End Using
            Catch ex As Exception
                reader?.Close()
                Logger.WriteLine(ex)
            End Try

            Return reader
        End Function

        ''' <summary>
        ''' 返回查询的DataReader结果，建议使用using语句调用,以确保使用完之后关闭reader,并且要在用完Reader之后 End Using 语句之前调用 Reader的CloseDbConnection扩展方法,确保相应的数据库连接在关闭
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Async Function GetDataReaderAsync(ByVal sql As String) As Task(Of DbDataReader)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await GetDataReaderAsync(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 根据sql语句返回结果，Object类型
        ''' </summary>
        ''' <param name="dbFileFullPath">数据库名</param>
        ''' <param name="sql">SQL语句</param>
        ''' <returns>只返回第一个满足条件的结果，没有数据返回Nothing</returns>
        Public Overloads Shared Async Function GetFirstAsync(ByVal dbFileFullPath As String, ByVal sql As String) As Task(Of Object)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim result As Object = Nothing


            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Dim sqliteCmd As New SQLiteCommand With {
                    .CommandText = sql,
                    .Connection = sqliteCn
                }

                result = Await sqliteCmd.ExecuteScalarAsync()
            End Using

            Return result
        End Function

        ''' <summary>
        ''' 根据sql语句返回结果，Object类型
        ''' </summary>
        ''' <param name="sql">SQL语句</param>
        ''' <returns>返回第一个满足条件的结果，没有数据返回Nothing</returns>
        Public Overloads Shared Async Function GetFirstAsync(ByVal sql As String) As Task(Of Object)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return Await GetFirstAsync(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 使用DataReader效率比DataAdapter高
        ''' </summary>
        ''' <param name="sqliteCmd"></param>
        ''' <returns></returns>
        Private Shared Async Function DataReaderAsync(ByVal sqliteCmd As SQLiteCommand) As Task(Of DataTable)
            Dim funcRst As New DataTable

            Using reader = Await sqliteCmd.ExecuteReaderAsync(CommandBehavior.CloseConnection)
                DataReaderInternal(reader, funcRst)
            End Using

            Return funcRst
        End Function

#End Region

#Region "同步函数区"
        ''' <summary>
        ''' SQLite建表
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql">建表语句</param>
        ''' <returns>成功 Success 返回True,失败 Success返回False,具体消息看 Message 参数</returns>
        Public Shared Function CreateTable(ByVal dbFileFullPath As String, ByVal sql As String) As (Success As Boolean, Message As String)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim funcRst As Boolean
            Dim message = String.Empty

            ' 创建DB数据库路径
            ' 如果该目录已存在，则不引发任何异常。
            IO2.Directory.Create(dbFileFullPath)

            Try
                Using sqliteCn As New SQLiteConnection
                    sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                    If sqliteCn.State = ConnectionState.Closed Then
                        sqliteCn.Open()
                    End If

                    Dim sqliteCmd As New SQLiteCommand With {
                        .CommandText = sql,
                        .Connection = sqliteCn
                        }

                    sqliteCmd.ExecuteNonQuery()
                End Using

                funcRst = True
                Debug.Print(Logger.MakeDebugString("建表过程完成"))

            Catch ex As AggregateException
                For Each innerEx As Exception In ex.InnerExceptions
                    Logger.WriteLine(innerEx)
                Next

                message = ex.Message
            End Try

            Return (funcRst, message)
        End Function
        ''' <summary>
        ''' 数据库是否存在某表
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="tableName"></param>
        ''' <returns></returns>
        Public Shared Function ExsitTable(ByVal dbFileFullPath As String, ByVal tableName As String) As Boolean
            Dim sql = $"select name from sqlite_master where name = '{tableName}'"
            Dim existTable = GetFirst(dbFileFullPath， sql)
            Return existTable IsNot Nothing
        End Function

        ''' <summary>
        ''' 数据库是否存在某表
        ''' </summary>
        ''' <param name="tableName"></param>
        ''' <returns></returns>
        Public Shared Function ExsitTable(ByVal tableName As String) As Boolean
            Return ExsitTable(s_DBFileFullPath, tableName)
        End Function

        ''' <summary>
        ''' 某表是否存在某字段
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="tableName"></param>
        ''' <param name="fieldName"></param>
        ''' <returns></returns>
        Public Shared Function ExsitField(ByVal dbFileFullPath As String, ByVal tableName As String, ByVal fieldName As String) As Boolean
            Dim sql = $"select sql from sqlite_master where name = '{tableName}'"
            Dim createTableSql = GetFirst(dbFileFullPath, sql)
            Dim pattern = $"(?!\w)\s*?""?{fieldName}""?\s+\w"
            Dim match = Regex.Match(createTableSql.ToString, pattern, RegexOptions.IgnoreCase Or RegexOptions.Compiled)

            Return match.Success
        End Function

        ''' <summary>
        ''' 某表是否存在某字段
        ''' </summary>
        ''' <param name="tableName"></param>
        ''' <param name="fieldName"></param>
        ''' <returns></returns>
        Public Shared Function ExsitField(ByVal tableName As String, ByVal fieldName As String) As Boolean
            Return ExsitField(s_DBFileFullPath, tableName, fieldName)
        End Function

        ''' <summary>
        ''' SQLite建表
        ''' </summary>
        ''' <param name="sql">建表语句</param>
        ''' <returns>成功 Success 返回True,失败 Success返回False,具体消息看 Message 参数</returns>
        Public Shared Function CreateTable(ByVal sql As String) As (Success As Boolean, Message As String)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return CreateTable(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作
        ''' unknown error
        ''' Insufficient parameters supplied To the command
        ''' 此错误一般是参数个数 或者参数名与字段名不一致造成
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <param name="sqliteCmd"></param>
        ''' <returns>成功——true，失败——false</returns>
        Public Overloads Shared Function ExecuteNonQuery(ByVal sql As String, ByVal sqliteCmd As SQLiteCommand) As Boolean
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return ExecuteNonQuery(s_DBFileFullPath, sql, sqliteCmd)
        End Function

        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作
        ''' unknown error
        ''' Insufficient parameters supplied To the command
        ''' 此错误一般是参数个数 或者参数名与字段名不一致造成
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <param name="sqliteCmd"></param>
        ''' <returns>成功——true，失败——false</returns>
        Public Overloads Shared Function ExecuteNonQuery(ByVal dbFileFullPath As String, ByVal sql As String, ByRef sqliteCmd As SQLiteCommand) As Boolean
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim result As Boolean

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)

                Debug.Print(Logger.MakeDebugString(sql))
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                sqliteCmd.Connection = sqliteCn
                sqliteCmd.CommandText = sql
                Try
                    Dim rst = sqliteCmd.ExecuteNonQuery()
                    result = rst > 0
                    Debug.Print(Logger.MakeDebugString("操作成功 = " & result.ToString))
                Catch ex As Exception
                    Logger.WriteLine(ex)
                End Try
            End Using

            Return result
        End Function
        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <returns>成功——true，失败——false</returns>
        Public Overloads Shared Function ExecuteNonQuery(ByVal sql As String) As Boolean
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return ExecuteNonQuery(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 执行 更新 添加 删除等查询操作
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <returns>成功——true，失败——false</returns>
        Public Overloads Shared Function ExecuteNonQuery(ByVal dbFileFullPath As String, ByVal sql As String) As Boolean
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim result As Boolean

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                Dim sqliteCmd As New SQLiteCommand(sql, sqliteCn)

                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Dim rst As Integer
                Try
                    rst = sqliteCmd.ExecuteNonQuery()
                Catch ex As Exception
                    Throw
                Finally
                    result = rst > 0
                End Try
            End Using

            Return result
        End Function


        ''' <summary>
        ''' 返回查询的DataReader结果，建议使用using语句调用,以确保使用完之后关闭reader,并且要在用完Reader之后 End Using 语句之前调用 Reader的CloseDbConnection扩展方法,确保相应的数据库连接在关闭
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Function GetDataReader(ByVal dbFileFullPath As String, ByVal sql As String) As DbDataReader
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            ' 为了减少调用者的外部dll依赖，所以 返回的是 DbDataReader 而不是 SQLiteDataReader
            Dim reader As DbDataReader = Nothing

            Dim sqliteCn As New SQLiteConnection With {
                    .connectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                }

            Try
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Using sqliteCmd = New SQLiteCommand With {
                    .CommandText = sql,
                    .Connection = sqliteCn
                }

                    reader = sqliteCmd.ExecuteReader(CommandBehavior.CloseConnection)
                End Using
            Catch ex As Exception
                reader?.Close()
                Logger.WriteLine(ex, sql,,,)
            End Try

            Return reader
        End Function

        ''' <summary>
        ''' 返回查询的DataReader结果，建议使用using语句调用,以确保使用完之后关闭reader,并且要在用完Reader之后 End Using 语句之前调用 Reader的CloseDbConnection扩展方法,确保相应的数据库连接在关闭
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Function GetDataReader(ByVal sql As String) As DbDataReader
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return GetDataReader(s_DBFileFullPath, sql)
        End Function


        ''' <summary>
        ''' 根据sql语句返回第一个满足条件的结果，Object类型
        ''' </summary>
        ''' <param name="dbFileFullPath">数据库名</param>
        ''' <param name="sql">SQL语句</param>
        ''' <returns>只返回第一个满足条件的结果，没有数据返回Nothing</returns>
        Public Overloads Shared Function GetFirst(ByVal dbFileFullPath As String, ByVal sql As String) As Object
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim result As Object = Nothing

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Dim sqliteCmd As New SQLiteCommand With {
                    .CommandText = sql,
                    .Connection = sqliteCn
                }

                result = sqliteCmd.ExecuteScalar()
            End Using

            Return result
        End Function

        ''' <summary>
        ''' 根据sql语句返回结果，Object类型
        ''' </summary>
        ''' <param name="sql">SQL语句</param>
        ''' <returns>返回第一个满足条件的结果，没有数据返回Nothing</returns>
        Public Overloads Shared Function GetFirst(ByVal sql As String) As Object
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return GetFirst(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 按表名和字段名获取相关数据
        ''' 只获取一个字段数据
        ''' </summary>
        ''' <param name="tableName">表名</param>
        ''' <param name="fieldName">字段名</param>
        ''' <param name="condition">查询条件</param>
        ''' <returns></returns>
        Public Overloads Shared Function GetData(ByVal tableName As String, ByVal fieldName As String， ByVal condition As String) As List(Of String)
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return GetData(s_DBFileFullPath, tableName, fieldName, condition)
        End Function

        ''' <summary>
        ''' 按表名和字段名获取相关数据
        ''' 只获取一个字段数据
        ''' </summary>
        ''' <param name="dbFileFullPath">数据库名</param>
        ''' <param name="tableName">表名</param>
        ''' <param name="fieldName">字段名</param>
        ''' <param name="condition">查询条件</param>
        ''' <returns></returns>
        Public Overloads Shared Function GetData(ByVal dbFileFullPath As String, ByVal tableName As String, ByVal fieldName As String， ByVal condition As String) As List(Of String)
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim result As New List(Of String)

            Using sqliteCn As New SQLiteConnection
                sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                If sqliteCn.State = ConnectionState.Closed Then
                    sqliteCn.Open()
                End If

                Dim sqliteCmd As New SQLiteCommand With {
                    .CommandText = $"select {fieldName} from {tableName} {If(condition.Length > 0, String.Concat("where ", condition), String.Empty)}",
                    .Connection = sqliteCn
                }

                Using reader = sqliteCmd.ExecuteReader(CommandBehavior.CloseConnection)
                    While reader.Read()
                        result.Add(CStr(reader(0)))
                    End While
                End Using
            End Using

            Return result
        End Function

        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Function GetData(ByVal sql As String) As DataGridView
            If String.IsNullOrEmpty(s_DBFileFullPath) Then
                Throw New SQLiteUnInitializeException(sqliteUnInitializeMessage)
            End If

            Return GetData(s_DBFileFullPath, sql)
        End Function

        ''' <summary>
        ''' 按照SQL语句查询
        ''' </summary>
        ''' <param name="dbFileFullPath"></param>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        Public Overloads Shared Function GetData(ByVal dbFileFullPath As String, ByVal sql As String) As DataGridView
            If String.IsNullOrEmpty(dbFileFullPath) Then
                Throw New ArgumentNullException(String.Format(My.Resources.NullReference, NameOf(dbFileFullPath)))
            End If

            Dim dgv As New DataGridView

            Debug.Print(Logger.MakeDebugString(sql))

            Try
                Using sqliteCn As New SQLiteConnection
                    sqliteCn.ConnectionString = String.Format(connectionStringTemplete, dbFileFullPath, s_Pooling.ToString, s_FailIfMissing.ToString)
                    If sqliteCn.State = ConnectionState.Closed Then
                        sqliteCn.Open()
                    End If

                    Dim sqliteCmd As New SQLiteCommand With {
                            .CommandText = sql,
                            .Connection = sqliteCn
                        }

                    Dim dt As DataTable = DataReader(sqliteCmd)

                    If dgv.InvokeRequired Then
                        dgv.Invoke(Sub() dgv.DataSource = dt)
                    Else
                        dgv.DataSource = dt
                    End If

                    If dgv.RowCount = 1 AndAlso dgv.Rows(0).Cells(1).Value Is Nothing Then
                        dgv.Rows(0).Selected = True
                    End If
                End Using
            Catch ex As AggregateException
                For Each innerex As Exception In ex.InnerExceptions
                    Logger.WriteLine(ex)
                Next
            End Try

            Return dgv
        End Function

        ''' <summary>
        ''' 使用DataReader效率比DataAdapter高
        ''' </summary>
        ''' <param name="sqliteCmd"></param>
        ''' <returns></returns>
        Private Shared Function DataReader(ByVal sqliteCmd As SQLiteCommand) As DataTable
            Dim funcRst As New DataTable

            Using reader = sqliteCmd.ExecuteReader(CommandBehavior.CloseConnection)
                DataReaderInternal(reader, funcRst)
            End Using

            Return funcRst
        End Function

        Private Shared Sub DataReaderInternal(ByVal reader As DbDataReader, ByRef table As DataTable)
            Dim col As DataColumn
            Dim row As DataRow

            For i As Integer = 0 To reader.FieldCount - 1
                col = New DataColumn With {
                    .ColumnName = reader.GetName(i),
                    .DataType = reader.GetFieldType(i)
                }

                table.Columns.Add(col)
            Next

            While reader.Read
                row = table.NewRow
                For i As Integer = 0 To reader.FieldCount - 1
                    row(i) = reader(i)
                Next
                table.Rows.Add(row)
            End While
        End Sub
#End Region
    End Class

End Namespace
