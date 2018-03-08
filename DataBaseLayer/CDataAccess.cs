using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace DataBaseLayer
{

    /// <summary>

    /// This class is used for providing DataBase Access using Microsoft.ApplicationBlocks.Data.

    /// </summary>

    public class CDataAccess 
    {
        private string strErrMsg = "";

        public string ErrMsg 
        {
            get
            {
                return strErrMsg;
            }
        }

        /// <summary>

        /// The default constructor.

        /// </summary>

        public CDataAccess()
        {
            
        }

        #region Properties

        public static string DBConnectionString
        {

            get
            {

                return "Data Source = " + ConfigurationSettings.AppSettings["DatabaseServer"] + " ; " +

                    "Initial Catalog = " + ConfigurationSettings.AppSettings["Database"] + " ; " +

                    "User ID = " + ConfigurationSettings.AppSettings["UserId"] + " ; " +

                    "Password = " + ConfigurationSettings.AppSettings["Password"] + " ";


            }

        }

        public static string CurrentThreadDBDateCommand
        {

            get
            {

                string sDateFormat = System.Threading.Thread.CurrentThread.CurrentCulture.DateTimeFormat.ShortDatePattern;

                return "SET DATEFORMAT " + (sDateFormat.IndexOf('M') < sDateFormat.IndexOf('d') ? (sDateFormat.IndexOf('M') < sDateFormat.IndexOf('y') ? "mdy" : "ymd") : "dmy") + "; ";

            }

        }

        /// <summary>

        /// The Transactional Isolation Level is set to READ COMMITTED.

        /// Used for some queries where there are small Select Queries. 

        /// </summary>

        public static string DBIsolationLevelREADCOMMITTED
        {
            get
            {
                return " SET TRANSACTION ISOLATION LEVEL " + ConfigurationSettings.AppSettings["TransactionIsolationLevelCommitted"] + " ";
            }

        }

        /// <summary>

        /// The Transactional Isolation Level is set to READ UNCOMMITTED.

        /// </summary>

        public static string DBIsolationLevelREADUNCOMMITTED
        {
            get
            {
                return " SET TRANSACTION ISOLATION LEVEL " + ConfigurationSettings.AppSettings["TransactionIsolationLevelUncommitted"] + " ";

            }

        }

        /// <summary>

        /// The Transactional Isolation Level used for some queries where there is small Select Queries. 

        /// </summary>

        public static string DBIsolationLevelREPEATABLEREAD
        {

            get
            {

                return " SET TRANSACTION ISOLATION LEVEL " + ConfigurationSettings.AppSettings["TransactionIsolationLevelRepeatable"] + " ";

            }

        }

        #endregion

        #region SQLExecutionFunctions

        public static int ExecuteNonQuery(CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteNonQuery(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText);

        }
        
        public static DataSet ExecuteDataSet(CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteDataset(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText);

        }

        public DataTable ExecuteDataTable(CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteDataTable(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText);

        }

        public DataTable ExecuteDataTable(string storedProcedure, SqlParameter[] arrPrm)
        {
            strErrMsg = "";
            DataTable dt = new DataTable();
            try
            {
                return SqlHelper.ExecuteDataTable(DBConnectionString, CommandType.StoredProcedure, storedProcedure);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }

        }

        public DataTable ExecuteDataTable(string storedProcedure, SqlParameter sqlParameter)
        {
            strErrMsg = "";
            DataTable dt = new DataTable();
            try
            {
                dt = SqlHelper.ExecuteDataTable(DBConnectionString, storedProcedure, sqlParameter);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }
            return dt;
        }

        public DataTable GetDataTable(string xCmdText, CommandType xCmdType)
        {
            strErrMsg = "";
            DataTable dt = new DataTable();
            try
            {
                dt = SqlHelper.ExecuteDataTable(DBConnectionString, xCmdType, xCmdText);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }
            return dt;
        }

        public DataTable GetDataTable(string xSPName, SqlParameter[] xArrPrm)
        {
            strErrMsg = "";
            DataTable dt = new DataTable();
            try
            {
                dt = SqlHelper.ExecuteDataTable(DBConnectionString, xSPName, xArrPrm);
                return dt;
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }
            return dt;
        }

        public DataTable GetDataTable(string xSPName, string xCompGroupCode)
        {
            strErrMsg = "";
            DataTable dt = new DataTable();
            try
            {
                SqlParameter objParameter = new SqlParameter("@VARCOMPGROUPCODE", xCompGroupCode);
                dt = SqlHelper.ExecuteDataTable(DBConnectionString, xSPName, objParameter);
                return dt;
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }
            return dt;
        }

        public DataTable GetDataTable(string xSPName, SqlParameter xSqlParameter1)
        {
            strErrMsg = "";
            DataTable dt = new DataTable();
            try
            {
                dt = SqlHelper.ExecuteDataTable(DBConnectionString, xSPName, xSqlParameter1);
                return dt;
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }
            return dt;
        }

        public object GetScalerValue(string xQry)
        {
            strErrMsg = "";
            object lResult;
            try
            {
                lResult = SqlHelper.ExecuteScalar(DBConnectionString, CommandType.Text, xQry);
                if (System.Convert.IsDBNull(lResult) == true) lResult = "";
            }
            catch (Exception Ex)
            {
                lResult = null;
                strErrMsg = Ex.Message;
            }
            return lResult;
        }

        public object GetScalerValue(string xSPName, SqlParameter[] xArrPrm)
        {
            strErrMsg = "";
            object lResult;
            try
            {
                lResult = SqlHelper.ExecuteScalar(DBConnectionString, xSPName, xArrPrm);
                if (System.Convert.IsDBNull(lResult) == true) lResult = "";
            }
            catch (Exception Ex)
            {
                lResult = null;
                strErrMsg = Ex.Message;
            }
            return lResult;
        }

        public static SqlDataReader ExecuteReader(CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteReader(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText);

        }

        public static object ExecuteScalar(CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteScalar(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, (SqlParameter[])null);

        }

        #endregion

        #region StoredProcedureExecutionFunctions

        public static string ExecuteNonQuery(CommandType oCommandType, string sCommandText, params SqlParameter[] commandParameters)
        {

            return SqlHelper.ExecuteNonQuery(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, commandParameters).ToString();

        }

        public bool ExecuteNonQuery(string _CommandText, CommandType _Commandtype)
        {
            strErrMsg = "";
            try
            {
                SqlHelper.ExecuteNonQuery(DBConnectionString, _Commandtype, _CommandText);
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
            finally
            {
                //cmd.Dispose();
            }
        }
        public bool ExecuteNonQuery(string _procedure, params SqlParameter[] objParam)
        {
            strErrMsg = "";
            try
            {
                SqlHelper.ExecuteNonQuery(DBConnectionString, _procedure, objParam);
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
            finally
            {
                //cmd.Dispose();
            }

        }


        public bool ExecuteNonQuery(string _procedure, int retpar, params SqlParameter[] objParam)
        {
            strErrMsg = "";
            try
            {
                SqlHelper.ExecuteNonQuery(DBConnectionString, _procedure, retpar, objParam);
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
            finally
            {
                //cmd.Dispose();
            }

        }

        public static DataSet ExecuteDataSet(CommandType oCommandType, string sCommandText, params SqlParameter[] commandParameters)
        {

            return SqlHelper.ExecuteDataset(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, commandParameters);

        }


        public static SqlDataReader ExecuteReader(CommandType oCommandType, string sCommandText, params SqlParameter[] commandParameters)
        {

            return SqlHelper.ExecuteReader(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, commandParameters);

        }

        public static object ExecuteScalar(CommandType oCommandType, string sCommandText, params SqlParameter[] commandParameters)
        {

            return SqlHelper.ExecuteScalar(DBConnectionString, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, commandParameters);

        }

        #endregion

        #region FunctionsRequiringConnection

        public static int ExecuteNonQuery(SqlConnection sqlconDBCon, CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteNonQuery(sqlconDBCon, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, (SqlParameter[])null);

        }

        public static object ExecuteScalar(SqlConnection sqlconDBCon, CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteScalar(sqlconDBCon, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, (SqlParameter[])null);

        }

        #endregion

        #region FunctionsRequiringTransaction

        public static int ExecuteNonQuery(SqlTransaction oSqlTransaction, CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteNonQuery(oSqlTransaction, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText);

        }


        public static object ExecuteScalar(SqlTransaction oSqlTransaction, CommandType oCommandType, string sCommandText)
        {

            return SqlHelper.ExecuteScalar(oSqlTransaction, oCommandType, (oCommandType == CommandType.Text) ? CurrentThreadDBDateCommand + sCommandText : sCommandText, (SqlParameter[])null);

        }

        #endregion

        #region CustomFunctions

        /// <summary>

        /// Overload of dataset to execute the parent child relationship.

        /// This is not present in DAAB.

        /// </summary>

        public static DataSet ExecuteDataSet(string sSQL, DataSet dsData, string sTableName)
        {

            SqlConnection sqlconConnection = new SqlConnection(DBConnectionString);

            SqlDataAdapter sqldtaAdapter = new SqlDataAdapter(CurrentThreadDBDateCommand + sSQL, sqlconConnection);

            if (dsData == null)

                dsData = new DataSet();

            sqldtaAdapter.Fill(dsData, sTableName);

            sqlconConnection.Close();

            return dsData;

        }

        #endregion

    }

}