Imports System.Runtime.Serialization


Namespace ShanXingTech.Exception2
    Public Class SQLiteInitializationException
        Inherits Exception

        Public Sub New(message As String)
            MyBase.New(message)
        End Sub

        Public Overrides Sub GetObjectData(info As SerializationInfo, context As StreamingContext)
            MyBase.GetObjectData(info, context)
        End Sub
    End Class

    Public Class SQLiteUnInitializeException
        Inherits Exception

        Public Sub New(message As String)
            MyBase.New(message)
        End Sub

        Public Overrides Sub GetObjectData(info As SerializationInfo, context As StreamingContext)
            MyBase.GetObjectData(info, context)
        End Sub
    End Class
End Namespace
