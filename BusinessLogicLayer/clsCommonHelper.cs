#region Use for NameSpace
using System;
using System.Collections;
using System.Data;
using System.Web.UI.WebControls;
using System.Data.SqlClient;
using DataBaseLayer;
using System.Net.Mail;
#endregion
namespace BusinessLogicLayer
{
    #region for Methord
    public class clsCommonHelper
    {

        /// <summary>
        /// Bind Grid when no row returnd by command.
        /// </summary>
        /// <param name="source"></param>
        /// Deepak Balhara:20-07-2016
        /// <param name="Gride View"></param>
        public void ShowNoResultFound(DataTable source, GridView gv)
        {
            source.Rows.Add(source.NewRow());
            gv.DataSource = source;
            gv.DataBind();
            int columnsCount = gv.Columns.Count;
            gv.Rows[0].Cells.Clear();
            gv.Rows[0].Cells.Add(new TableCell());
            gv.Rows[0].Cells[0].ColumnSpan = columnsCount;
            gv.Rows[0].Cells[0].HorizontalAlign = HorizontalAlign.Center;
            gv.Rows[0].Cells[0].ForeColor = System.Drawing.Color.Red;
            gv.Rows[0].Cells[0].Font.Bold = true;
            gv.Rows[0].Cells[0].Text = "NO DATA FOUND, PLEASE ADD NEW ONE.";
        }

        public DataTable GetImage(string imgId)
        {
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                SqlParameter[] objParameter = new SqlParameter[1];
                objParameter[0] = new SqlParameter("@pUID", SqlDbType.Int); objParameter[0].Value = imgId;
                //objParameter[1] = new SqlParameter("@pColumnName", SqlDbType.VarChar, 50); objParameter[1].Value = _colomnName;
                //objParameter[2] = new SqlParameter("@pTableName", SqlDbType.VarChar, 50); objParameter[2].Value = _tableName;
                return objDataAccessHelper.GetDataTable("GetImages", objParameter);
            }
            catch (Exception)
            {
                return null;
            }
        }



        /// <summary>
        /// Get images from database 
        /// Deepak Balhara:21-07-2016
        /// </summary>
        /// <param name="imgId"></param>
        /// <param name="_colomnName"></param>
        /// <param name="_tableName"></param>
        /// <returns></returns>
        public DataTable GetImages(string imgId, string _colomnName, string _tableName)
        {
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                SqlParameter[] objParameter = new SqlParameter[3];
                objParameter[0] = new SqlParameter("@pUID", SqlDbType.Int); objParameter[0].Value = imgId;
                objParameter[1] = new SqlParameter("@pColumnName", SqlDbType.VarChar, 50); objParameter[1].Value = _colomnName;
                objParameter[2] = new SqlParameter("@pTableName", SqlDbType.VarChar, 50); objParameter[2].Value = _tableName;
                return objDataAccessHelper.GetDataTable("GetImages", objParameter);
            }
            catch (Exception)
            {
                return null;
            }
        }




        /// <summary>
        /// class for manage page listing
        /// Deepak Balhara:21-07-2016
        /// </summary>
        /// <param name="psearchtext"></param>
        /// <param name="pSearchby"></param>
        /// <param name="createdby"></param>
        /// <param name="_procedureName"></param>
        /// <returns></returns>
        public System.Data.DataTable GetSearchingListing(string psearchtext, int pSearchby, string createdby, string _procedureName)
        {
            DataSet _myDataSet = new DataSet();
            try
            {

                SqlParameter[] oParameters = new SqlParameter[5];

                oParameters[0] = new SqlParameter("@pvcSerachField", SqlDbType.VarChar);
                oParameters[0].Value = psearchtext;

                oParameters[1] = new SqlParameter("@pvcSortBy", SqlDbType.VarChar);
                oParameters[1].Value = "";

                oParameters[2] = new SqlParameter("@pvcSortOrder", SqlDbType.VarChar);
                oParameters[2].Value = "";

                oParameters[3] = new SqlParameter("@pintStartingWith", SqlDbType.Int);
                oParameters[3].Value = pSearchby;

                oParameters[4] = new SqlParameter("@pvcCreatedby", SqlDbType.VarChar);
                oParameters[4].Value = createdby;

                //if (createdby.Equals(string.Empty))
                //    oParameters[4].Value = DBNull.Value;
                //else

                _myDataSet = CDataAccess.ExecuteDataSet(System.Data.CommandType.StoredProcedure, _procedureName, oParameters);

                return _myDataSet.Tables[0];
            }
            catch (Exception)
            {
                return _myDataSet.Tables[0];
            }


        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="psearchtext"></param>
        /// <param name="pSearchby"></param>
        /// <param name="createdby"></param>
        /// <param name="Session"></param>
        /// <param name="_procedureName"></param>
        /// <returns></returns>

        public System.Data.DataTable GetSearchingListing1(string psearchtext, int pSearchby, string createdby, string Session, string _procedureName)
        {
            DataSet _myDataSet = new DataSet();
            try
            {

                SqlParameter[] oParameters = new SqlParameter[6];

                oParameters[0] = new SqlParameter("@pvcSerachField", SqlDbType.VarChar);
                oParameters[0].Value = psearchtext;

                oParameters[1] = new SqlParameter("@pvcSortBy", SqlDbType.VarChar);
                oParameters[1].Value = "";

                oParameters[2] = new SqlParameter("@pvcSortOrder", SqlDbType.VarChar);
                oParameters[2].Value = "";

                oParameters[3] = new SqlParameter("@pintStartingWith", SqlDbType.Int);
                oParameters[3].Value = pSearchby;

                oParameters[4] = new SqlParameter("@pvcCreatedby", SqlDbType.VarChar);
                oParameters[4].Value = createdby;

                oParameters[5] = new SqlParameter("@pvrSession", SqlDbType.BigInt);
                oParameters[5].Value = Session;


                //if (createdby.Equals(string.Empty))
                //    oParameters[4].Value = DBNull.Value;
                //else

                _myDataSet = CDataAccess.ExecuteDataSet(System.Data.CommandType.StoredProcedure, _procedureName, oParameters);

                return _myDataSet.Tables[0];
            }
            catch (Exception)
            {
                return _myDataSet.Tables[0];
            }


        }

        //public int DeleteRecords(string pUID, string pvcTable_name)
        //{
        //    int Results;
        //    SqlParameter[] oparameters = new SqlParameter[3];
        //    oparameters[0] = new SqlParameter("@pUID", SqlDbType.VarChar);
        //    oparameters[0].Value = pUID;
        //    oparameters[1] = new SqlParameter("@pTABLE_NAME", SqlDbType.VarChar);
        //    oparameters[1].Value = pvcTable_name;
        //    oparameters[2] = new SqlParameter("@pIntCheck", SqlDbType.Int);
        //    oparameters[2].Direction = ParameterDirection.Output;
        //    CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "DeleteProcedureCommon", oparameters);
        //    Results = Convert.ToInt32(oparameters[2].Value);
        //    return Results;
        //}
        //public string ChangeStatus(int pid, short pStatus, string pTableName, string pColumn)
        //{
        //    try
        //    {
        //        string Result;
        //        SqlParameter[] oParameters = new SqlParameter[5];

        //        oParameters[0] = new SqlParameter("@pvcTableName", SqlDbType.VarChar);//@pbintId	
        //        oParameters[0].Value = pTableName;

        //        oParameters[1] = new SqlParameter("@pnID", SqlDbType.BigInt);
        //        oParameters[1].Value = pid;

        //        oParameters[2] = new SqlParameter("@pbtStatus", SqlDbType.Bit);
        //        oParameters[2].Value = pStatus;

        //        oParameters[3] = new SqlParameter("@pColumn", SqlDbType.VarChar);
        //        oParameters[3].Value = pColumn;
        //        oParameters[4] = new SqlParameter("@pintCheck", SqlDbType.Int);
        //        oParameters[4].Direction = ParameterDirection.Output;
        //        CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "ColumnStatusUpdate_SP", oParameters);
        //        Result = oParameters[4].Value.ToString();
        //        return Result;
        //    }
        //    catch (Exception ex)
        //    {
        //        //throw ex;
        //        return "error";
        //    }
        //}

        /// <summary>
        /// Deepak Balhara:22-07-2016
        /// Delete records table-wise
        /// </summary>
        /// <param name="pUID"></param>
        /// <param name="ptablename"></param>
        public void Delete(string pUID, string ptablename)
        {
            try
            {
                SqlParameter[] oParameters = new SqlParameter[2];
                oParameters[0] = new SqlParameter("@pUID", SqlDbType.VarChar);
                oParameters[0].Value = pUID;
                oParameters[1] = new SqlParameter("@pTABLE_NAME", SqlDbType.VarChar);
                oParameters[1].Value = ptablename;
                CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "Table_Wise_Delete", oParameters);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Deepak Balhara:24-07-2016
        /// Deactivate records table-wise
        /// </summary>
        ///<param name="pUID"></param>
        ///<param name="ptablename"></param>
        public void Deactivate(string pUID, string ptablename)
        {
            try
            {
                SqlParameter[] oParameters = new SqlParameter[2];
                oParameters[0] = new SqlParameter("@pUID", SqlDbType.VarChar);
                oParameters[0].Value = pUID;
                oParameters[1] = new SqlParameter("@pTABLE_NAME", SqlDbType.VarChar);
                oParameters[1].Value = ptablename;
                CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "Table_Wise_Deactivate", oParameters);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Deepak Balhara:27-07-2016
        /// Activate records table-wise
        /// </summary>
        /// <param name="pUID"></param>
        /// <param name="ptablename"></param>
        public void Activate(string pUID, string ptablename)
        {
            try
            {
                SqlParameter[] oParameters = new SqlParameter[2];
                oParameters[0] = new SqlParameter("@pUID", SqlDbType.VarChar);
                oParameters[0].Value = pUID;
                oParameters[1] = new SqlParameter("@pTABLE_NAME", SqlDbType.VarChar);
                oParameters[1].Value = ptablename;
                CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "Table_Wise_Activate", oParameters);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Deepak Balhara:28-07-2016
        /// Activate records table-wise
        /// </summary>
        /// <param name="ServerName"></param>
        /// <param name="DatabaseName"></param>
        /// <param name="UserID"></param>
        /// <param name="Password"></param>
        private string strErrMsg = "";
        public string ErrMsg
        {
            get
            {
                return strErrMsg;
            }
        }
        public Hashtable GetDatabaseInfo()
        {
            strErrMsg = "";
            try
            {
                Hashtable objHashTab = new Hashtable();
                objHashTab.Add("ServerName", System.Configuration.ConfigurationSettings.AppSettings["DatabaseServer"].ToString());
                objHashTab.Add("UserID", System.Configuration.ConfigurationSettings.AppSettings["UserId"].ToString());
                objHashTab.Add("Password", System.Configuration.ConfigurationSettings.AppSettings["Password"].ToString());
                objHashTab.Add("DatabaseName", System.Configuration.ConfigurationSettings.AppSettings["Database"].ToString());
                return objHashTab;
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }
            return null;
        }


        /// <summary>
        /// Deepak Balhara:30-07-2016
       
        /// Fill Data Table without Parameter
        /// </summary>
        /// <param name="_query"></param>
        /// <returns></returns>
        public DataTable FillDataTable(string _query)
        {
            strErrMsg = "";
            CDataAccess objDataAccessHelper = new CDataAccess();
            DataTable _dt = new DataTable();
            try
            {
                _dt = objDataAccessHelper.GetDataTable(_query, CommandType.Text);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }

            return _dt;
        }

        /// <summary>
        /// Deepak Balhara:04-08-2016
        /// Fill Data Table with Query
        /// </summary>
        ///<param name="_query"></param>
        ///<param name="_commandType"></param>
        /// <returns></returns>
        public DataTable FillDataTable(string _query, CommandType _commandType)
        {
            strErrMsg = "";
            CDataAccess objDataAccessHelper = new CDataAccess();
            DataTable _dt = new DataTable();
            try
            {
                _dt = objDataAccessHelper.GetDataTable(_query, _commandType);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
            }
            return _dt;
        }

        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// Fill Data Table with Parameter
        /// </summary>  
        /// <param name="_procedureName"></param>
        /// <param name="_StyleKey"></param>
        /// <returns></returns>
        public DataTable FillDataTable(string _procedureName, string _StyleKey)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                SqlParameter[] objBuyerParameter = new SqlParameter[1];
                objBuyerParameter[0] = new SqlParameter("@pStyle_Key", SqlDbType.VarChar, 20);
                objBuyerParameter[0].Value = _StyleKey;

                return objDataAccessHelper.GetDataTable(_procedureName, objBuyerParameter);

            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }


        }

        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// Fill Data Table with Parameter
        /// </summary>
        /// <param name="_procedureName"></param>
        /// <param name="_ClassKey"></param>
        /// <param name="_SectionKey"></param>
        /// <returns></returns>
        public DataTable FillDataTable(string _procedureName, string _AKey, string _BKey)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();

                SqlParameter[] objBuyerParameter = new SqlParameter[2];

                objBuyerParameter[0] = new SqlParameter("@pAKey", SqlDbType.NVarChar, 20);
                objBuyerParameter[1] = new SqlParameter("@pBKey", SqlDbType.NVarChar, 20);

                objBuyerParameter[0].Value = _AKey;
                objBuyerParameter[1].Value = _BKey;

                return objDataAccessHelper.GetDataTable(_procedureName, objBuyerParameter);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// Fill Data Table with Parameter
        /// </summary>
        /// <param name="_procedureName"></param>
        /// <param name="_ClassKey"></param>
        /// <param name="_SectionKey"></param>
        /// <returns></returns>
        public DataTable FillDataTable(string _procedureName, string _CKey, string _DKey, string _EKey)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();

                SqlParameter[] objBuyerParameter = new SqlParameter[3];

                objBuyerParameter[0] = new SqlParameter("@pCKey", SqlDbType.NVarChar, 20);
                objBuyerParameter[1] = new SqlParameter("@pDKey", SqlDbType.NVarChar, 20);
                objBuyerParameter[2] = new SqlParameter("@pEKey", SqlDbType.NVarChar, 20);

                objBuyerParameter[0].Value = _CKey;
                objBuyerParameter[1].Value = _DKey;
                objBuyerParameter[2].Value = _EKey;

                return objDataAccessHelper.GetDataTable(_procedureName, objBuyerParameter);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// Fill Data Table with Parameter
        /// </summary>
        /// <param name="_procedureName"></param>
        /// <param name="_ClassKey"></param>
        /// <param name="_SectionKey"></param>
        /// <returns></returns>

        public DataTable FillDataTable(string _procedureName, string _FKey, string _GKey, string _HKey, string _IKey)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();

                SqlParameter[] objBuyerParameter = new SqlParameter[4];

                objBuyerParameter[0] = new SqlParameter("@pFKey", SqlDbType.NVarChar, 20);
                objBuyerParameter[1] = new SqlParameter("@pGKey", SqlDbType.NVarChar, 20);
                objBuyerParameter[2] = new SqlParameter("@pHKey", SqlDbType.NVarChar, 20);
                objBuyerParameter[3] = new SqlParameter("@pIKey", SqlDbType.NVarChar, 20);

                objBuyerParameter[0].Value = _FKey;
                objBuyerParameter[1].Value = _GKey;
                objBuyerParameter[2].Value = _HKey;
                objBuyerParameter[3].Value = _IKey;

                return objDataAccessHelper.GetDataTable(_procedureName, objBuyerParameter);
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// Fill Data Table with Parameter
        /// </summary>
        /// <param name="_procedureName"></param>
        /// <param name="_ClassKey"></param>
        /// <param name="_SectionKey"></param>
        /// <returns></returns>

        public DataTable FillDataTable(string _procedureName, string _HKey, string _JKey, string _KKey, string _LKey, string _MKey)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();

                SqlParameter[] objBuyerParameter = new SqlParameter[5];

                objBuyerParameter[0] = new SqlParameter("@pHKey", SqlDbType.VarChar, 20);
                objBuyerParameter[1] = new SqlParameter("@pJKey", SqlDbType.VarChar, 20);
                objBuyerParameter[2] = new SqlParameter("@pKKey", SqlDbType.VarChar, 20);
                objBuyerParameter[3] = new SqlParameter("@pLKey", SqlDbType.VarChar, 20);
                objBuyerParameter[4] = new SqlParameter("@pMKey", SqlDbType.VarChar, 20);


                objBuyerParameter[0].Value = _HKey;
                objBuyerParameter[1].Value = _JKey;
                objBuyerParameter[2].Value = _KKey;
                objBuyerParameter[3].Value = _LKey;
                objBuyerParameter[4].Value = _MKey;

                return objDataAccessHelper.GetDataTable(_procedureName, objBuyerParameter);

            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }
        }

        public DataTable FillDataTable(string _procedureName, string _HKey, string _JKey, string _KKey, string _LKey, string _MKey, string _NKey)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();

                SqlParameter[] objBuyerParameter = new SqlParameter[6];

                objBuyerParameter[0] = new SqlParameter("@pHKey", SqlDbType.VarChar, 20);
                objBuyerParameter[1] = new SqlParameter("@pJKey", SqlDbType.VarChar, 20);
                objBuyerParameter[2] = new SqlParameter("@pKKey", SqlDbType.VarChar, 20);
                objBuyerParameter[3] = new SqlParameter("@pLKey", SqlDbType.VarChar, 20);
                objBuyerParameter[4] = new SqlParameter("@pMKey", SqlDbType.VarChar, 20);
                objBuyerParameter[5] = new SqlParameter("@pMKey", SqlDbType.VarChar, 20);

                objBuyerParameter[0].Value = _HKey;
                objBuyerParameter[1].Value = _JKey;
                objBuyerParameter[2].Value = _KKey;
                objBuyerParameter[3].Value = _LKey;
                objBuyerParameter[4].Value = _MKey;
                objBuyerParameter[5].Value = _NKey;

                return objDataAccessHelper.GetDataTable(_procedureName, objBuyerParameter);

            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }
        }
        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// Fill Data Table with Parameter
        /// </summary>
        /// <param name="xCompGrpCode"></param>
        /// <param naparam name="xTblName"></param>
        /// <param name="xFldName"></param>
        /// <returns></returns>

        public string GetNextPK(string xCompGrpCode, string xTblName, string xFldName)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                SqlParameter[] objPara = new SqlParameter[3];
                objPara[0] = new SqlParameter("@pvarCompGroupCode", xCompGrpCode);
                objPara[1] = new SqlParameter("@pTblNm", xTblName);
                objPara[2] = new SqlParameter("@pFldNm", xFldName);
                return xCompGrpCode + objDataAccessHelper.GetScalerValue("UDF_SaS_NextPK", objPara).ToString();
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }

        }

        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// </summary>
        /// <param name="xTblName"></param>
        /// <param name="xFldName"></param>
        /// <param name="xHead_PkeyFldNm"></param>
        /// <param name="xHead_PkeyFldVal"></param>
        /// <returns></returns>
        public string GetNextPK_Detail(string xTblName, string xFldName, string xHead_PkeyFldNm, string xHead_PkeyFldVal)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                SqlParameter[] objPara = new SqlParameter[4];
                objPara[0] = new SqlParameter("@pTblNm", xTblName);
                objPara[1] = new SqlParameter("@pFldNm", xFldName);
                objPara[2] = new SqlParameter("@pHead_PkeyFldNm", xHead_PkeyFldNm);
                objPara[3] = new SqlParameter("@pHead_PkeyFldVal", xHead_PkeyFldVal);
                return objDataAccessHelper.GetScalerValue("UDF_SaS_NextPK_Detail", objPara).ToString();
            }
            catch (Exception Ex)
            {
                strErrMsg = Ex.Message;
                return null;
            }

        }

        /// <summary>
        /// Deepak Balhara:06-08-2016
        /// </summary>
        /// <param name="xDate"></param>
        /// <returns></returns>
        public string Format_yyyyMMdd(string xDate)
        {
            if (string.IsNullOrEmpty(xDate)) return "1900/01/01";
            string dd = xDate.Substring(0, 2);
            string mm = xDate.Substring(3, 2);
            string yy = xDate.Substring(6, 4);
            string value = yy + '/' + mm + '/' + dd.ToString();
            return (value);
        }
        /// <summary>
        /// Deepak Balhara:15-07-2016
        /// </summary>
        /// <param name="xDate"></param>
        /// <returns></returns>
        public string GetUniversalDateFormat(string xDate)
        {
            if (xDate == "")
            {
                xDate = "01/01/1900";
            }
            else
            {
                DateTime dt = DateTime.ParseExact(xDate, "dd/MM/yyyy", System.Globalization.CultureInfo.CurrentUICulture);
                return string.Format("{0:yyyy}", dt) + string.Format("{0:MM}", dt) + string.Format("{0:dd}", dt);
            }
            return string.Format(xDate);

        }


        /// <summary>
        /// Deepak Balhara:15-07-2016
        /// </summary>
        /// <param name="_strValue"></param>
        /// <returns></returns>
        public string HandleSpecialChar(string _strValue)
        {
            //************* Handle Special Character ************* 
            _strValue = _strValue.Trim();
            if (_strValue.Length != 0)
            {
                _strValue = _strValue.Replace("'", "");
                _strValue = _strValue.Replace(";", ",");
                _strValue = _strValue.Replace("//", "/");
                _strValue = _strValue.Replace("--", "");
                _strValue = _strValue.Replace("[", "");
                _strValue = _strValue.Replace("]", "");
            }
            else
            {
                _strValue = "";
            }
            return _strValue;
        }

        /// <summary>
        /// Deepak Balhara:15-07-2016
        /// </summary>
        /// <param name="xExtension"></param>
        /// <returns></returns>
        public bool IsValidImage(string xExtension)
        {
            xExtension = xExtension.ToLower();
            if (xExtension == ".jpg" || xExtension == ".jpeg" || xExtension == ".gif" ||
                xExtension == ".bmp" || xExtension == ".png" || xExtension == ".ico")
            {

                return true;
            }
            else
            {
                //ScriptManager.RegisterStartupScript(this, this.GetType(), "alert", "alert('Invalid file extension. please select valid File.');", true);
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:15-07-2016
        /// </summary>
        /// <param name="xExtension"></param>
        /// <returns></returns>
        public bool IsValidAttachedFile(string xExtension)
        {
            xExtension = xExtension.ToLower();
            if (xExtension == ".jpg" || xExtension == ".jpeg" || xExtension == ".gif" ||
                xExtension == ".bmp" || xExtension == ".htm" || xExtension == ".htMiddleLayer" ||
                xExtension == ".xls" || xExtension == ".xlsx" || xExtension == ".csv" ||
                xExtension == ".doc" || xExtension == ".docx" || xExtension == ".rtf" ||
                xExtension == ".pdf" || xExtension == ".txt" || xExtension == ".rar" || xExtension == ".zip")
            {

                return true;
            }
            else
            {
                //ScriptManager.RegisterStartupScript(this, this.GetType(), "alert", "alert('Invalid file extension. please select valid File.');", true);
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:15-07-2016
        /// </summary>
        /// <param name="_query"></param>
        /// <returns></returns>
        public object ExecuteScalar(string _query)
        {
            CDataAccess objDataAccessHelper = new CDataAccess();
            try
            {
                return objDataAccessHelper.GetScalerValue(_query);
            }
            catch (Exception)
            {
                return "error";
            }
        }

        /// <summary>
        /// Deepak Balhara:15-07-2016
        /// </summary>
        /// <param name="_query"></param>
        /// <returns></returns>
        public SqlDataReader subquery(string _query)
        {
            return CDataAccess.ExecuteReader(CommandType.Text, _query);
        }


        #region "Control Filling"
        ///----------------------------------------------------------Start DropDown---------------------------------------------------------------------

        /// Start DropDown List Filling

        /// <summary>
        /// Deepak Balhara:17-07-2016
        /// </summary>
        /// <param name="xDDL"></param>
        /// <param name="xQry"></param>
        /// <returns></returns>
        public bool FillDDL(DropDownList xDDL, string xQry)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                DataTable xDt = objDataAccessHelper.GetDataTable(xQry, CommandType.Text);
                objDataAccessHelper = null;
                xDDL.DataSource = xDt;
                xDDL.DataMember = xDt.TableName;
                xDDL.DataTextField = xDt.Columns[0].ColumnName;
                if (xDt.Columns.Count > 1)
                {
                    xDDL.DataValueField = xDt.Columns[1].ColumnName;
                }
                else
                {
                    xDDL.DataValueField = xDt.Columns[0].ColumnName;
                }
                xDDL.DataBind();
                xDDL.Items.Insert(-1, new ListItem("Choose", "-1"));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:17-07-2016
        /// </summary>
        /// <param name="xDDL"></param>
        /// <param name="xCommandName"></param>
        /// <param name="xCommandType"></param>
        /// <returns></returns>
        public bool FillDDL(DropDownList xDDL, string xCommandName, CommandType xCommandType)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                DataTable xDt = objDataAccessHelper.GetDataTable(xCommandName, xCommandType);
                objDataAccessHelper = null;
                xDDL.DataSource = xDt;
                xDDL.DataMember = xDt.TableName;
                xDDL.DataTextField = xDt.Columns[0].ColumnName;
                if (xDt.Columns.Count > 1)
                {
                    xDDL.DataTextField = xDt.Columns[0].ColumnName;
                    xDDL.DataValueField = xDt.Columns[1].ColumnName;

                }
                else
                {
                    xDDL.DataValueField = xDt.Columns[0].ColumnName;
                }
                xDDL.DataBind();
                xDDL.Items.Insert(0, new ListItem("Choose", null));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:17-07-2016
        /// </summary>
        /// <param name="xDDL"></param>
        /// <param name="xCommandName"></param>
        /// <param name="xCommandType"></param>
        /// <param name="xTxtFld"></param>
        /// <param name="xValFld"></param>
        /// <returns></returns>
        public bool FillDDL(DropDownList xDDL, string xCommandName, CommandType xCommandType, string xTxtFld, string xValFld)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                DataTable xDt = objDataAccessHelper.GetDataTable(xCommandName, xCommandType);
                objDataAccessHelper = null;
                xDDL.DataSource = xDt;
                xDDL.DataMember = xDt.TableName;
                xDDL.DataTextField = xTxtFld;
                xDDL.DataValueField = xValFld;
                xDDL.DataBind();
                xDDL.Items.Insert(0, new ListItem("Choose", null));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:17-07-2016
        /// </summary>
        /// <param name="xDDL"></param>
        /// <param name="xDt"></param>
        public void FillDDL(DropDownList xDDL, DataTable xDt)
        {
            strErrMsg = "";
            try
            {
                if (xDt.Rows.Count > 0)
                {
                    xDDL.SelectedIndex = -1;
                    xDDL.DataSource = xDt;
                    xDDL.DataMember = xDt.TableName;
                    xDDL.DataTextField = xDt.Columns[0].ColumnName;
                    if (xDt.Columns.Count > 1)
                    {
                        xDDL.DataValueField = xDt.Columns[1].ColumnName;
                    }
                    else
                    {
                        xDDL.DataValueField = xDt.Columns[0].ColumnName;
                    }
                    xDDL.DataBind();
                }
                xDDL.Items.Insert(0, new ListItem("Choose", "-1"));
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
            }
        }

        /// <summary>
        /// Deepak Balhara:18-07-2016
        /// </summary>
        /// <param name="xDDL"></param>
        /// <param name="xDt"></param>
        /// <param name="xTxtFld"></param>
        /// <param name="xValFld"></param>
        /// <returns></returns>
        public bool FillDDL(DropDownList xDDL, DataTable xDt, string xTxtFld, string xValFld)
        {
            strErrMsg = "";
            try
            {
                xDDL.DataSource = xDt;
                xDDL.DataMember = xDt.TableName;
                xDDL.DataTextField = xTxtFld;
                xDDL.DataValueField = xValFld;
                xDDL.DataBind();
                xDDL.Items.Insert(0, new ListItem("Choose", null));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:18-07-2016
        /// </summary>
        /// <param name="_ddllist"></param>
        /// <param name="_dtView"></param>
        /// <returns></returns>
        public bool FillDDL(DropDownList _ddllist, DataView _dtView)
        {
            strErrMsg = "";
            try
            {
                //if (_dtView.Table.Rows.Count > 0)
                //{
                _ddllist.DataSource = _dtView;
                _ddllist.DataTextField = _dtView.Table.Columns[0].ColumnName;
                if (_dtView.Table.Columns.Count > 1)
                {
                    _ddllist.DataValueField = _dtView.Table.Columns[1].ColumnName;
                }
                else
                {
                    _ddllist.DataValueField = _dtView.Table.Columns[0].ColumnName;
                }
                _ddllist.DataBind();
                _ddllist.Items.Insert(0, new ListItem("--Select--", "0"));
                return true;
                //}
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
            //return false;
        }

        /// <summary>
        /// DDeepak Balhara:18-07-2016
        /// </summary>
        /// <param name="_ddllist"></param>
        /// <param name="_dtView"></param>
        /// <param name="xTxtFld"></param>
        /// <param name="xValFld"></param>
        /// <returns></returns>
        public bool FillDDL(DropDownList _ddllist, DataView _dtView, string xTxtFld, string xValFld)
        {
            strErrMsg = "";
            try
            {
                
                //if (_dtView.Table.Rows.Count > 0)
                //{
                _ddllist.DataSource = _dtView;
                _ddllist.DataValueField = xValFld;
                _ddllist.DataTextField = xTxtFld;
                _ddllist.DataBind();
                _ddllist.Items.Insert(0, new ListItem("--Select--", "0"));
                return true;
                //}
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
            // return false;
        }

        //----------------------------------------------------------------End DropDown List Filling---------------------------------------------------------------------//

        //----------------------------------------------------------------Start Checkbox List Filling-------------------------------------------------------------------//



        //Start CheckBox List Filling


        /// <summary>
        /// Deepak Balhara:18-07-2016
        /// </summary>
        /// <param name="xCBL"></param>
        /// <param name="xQry"></param>
        /// <returns></returns>

        public bool FillCheckBoxList(CheckBoxList xCBL, string xQry)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                DataTable xDt = objDataAccessHelper.GetDataTable(xQry, CommandType.Text);
                objDataAccessHelper = null;
                xCBL.DataSource = xDt;
                xCBL.DataMember = xDt.TableName;
                xCBL.DataTextField = xDt.Columns[0].ColumnName;
                if (xDt.Columns.Count > 1)
                {
                    xCBL.DataValueField = xDt.Columns[1].ColumnName;
                }
                else
                {
                    xCBL.DataValueField = xDt.Columns[0].ColumnName;
                }
                xCBL.DataBind();
                xCBL.Items.Insert(0, new ListItem("--Select--", "0"));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }
        /// <summary>
        /// Deepak Balhara:18-07-2016
        /// </summary>
        /// <param name="xCBL"></param>
        /// <param name="xCommandName"></param>
        /// <param name="xCommandType"></param>
        /// <returns></returns>

        public bool FillCheckBoxList(CheckBoxList xCBL, string xCommandName, CommandType xCommandType)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                DataTable xDt = objDataAccessHelper.GetDataTable(xCommandName, xCommandType);
                objDataAccessHelper = null;
                xCBL.DataSource = xDt;
                xCBL.DataMember = xDt.TableName;
                xCBL.DataTextField = xDt.Columns[0].ColumnName;
                if (xDt.Columns.Count > 1)
                {
                    xCBL.DataValueField = xDt.Columns[1].ColumnName;
                }
                else
                {
                    xCBL.DataValueField = xDt.Columns[0].ColumnName;
                }
                xCBL.DataBind();
                xCBL.Items.Insert(0, new ListItem("--Select--", "0"));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:19-07-2016
        /// </summary>
        /// <param name="xCBL"></param>
        /// <param name="xCommandName"></param>
        /// <param name="xCommandType"></param>
        /// <param name="xTxtFld"></param>
        /// <param name="xValFld"></param>
        /// <returns></returns>
        public bool FillCheckBoxList(CheckBoxList xCBL, string xCommandName, CommandType xCommandType, string xTxtFld, string xValFld)
        {
            strErrMsg = "";
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                DataTable xDt = objDataAccessHelper.GetDataTable(xCommandName, xCommandType);
                objDataAccessHelper = null;
                xCBL.DataSource = xDt;
                xCBL.DataMember = xDt.TableName;
                xCBL.DataTextField = xTxtFld;
                xCBL.DataValueField = xValFld;
                xCBL.DataBind();
                xCBL.Items.Insert(0, new ListItem("--Select--", "0"));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Deepak Balhara:19-07-2016
        /// </summary>
        /// <param name="xCBL"></param>
        /// <param name="xDt"></param>
        public void FillCheckBoxList(CheckBoxList xCBL, DataTable xDt)
        {
            strErrMsg = "";
            try
            {
                xCBL.DataSource = xDt;
                xCBL.DataMember = xDt.TableName;
                xCBL.DataTextField = xDt.Columns[0].ColumnName;
                if (xDt.Columns.Count > 1)
                {
                    xCBL.DataValueField = xDt.Columns[1].ColumnName;
                }
                else
                {
                    xCBL.DataValueField = xDt.Columns[0].ColumnName;
                }
                xCBL.DataBind();
                xCBL.Items.Insert(0, new ListItem("--Select--", "0"));
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
            }
        }

        /// <summary>
        /// Deepak Balhara:19-07-2016
        /// </summary>
        /// <param name="xCBL"></param>
        /// <param name="xDt"></param>
        /// <param name="xTxtFld"></param>
        /// <param name="xValFld"></param>
        /// <returns></returns>
        public bool FillCheckBoxList(CheckBoxList xCBL, DataTable xDt, string xTxtFld, string xValFld)
        {
            strErrMsg = "";
            try
            {
                xCBL.DataSource = xDt;
                xCBL.DataMember = xDt.TableName;
                xCBL.DataTextField = xTxtFld;
                xCBL.DataValueField = xValFld;
                xCBL.DataBind();
                xCBL.Items.Insert(0, new ListItem("--Select--", "0"));
                return true;
            }
            catch (Exception ex)
            {
                strErrMsg = ex.Message;
                return false;
            }
        }

        //---------------------------------------------------End CheckBOX List Filling------------------------------------------------------------------//
        /// <summary>
        /// Deepak Balhara:19-07-2016
        /// </summary>
        /// <param name="_CommandText"></param>
        /// <param name="_Commandtype"></param>
        /// <returns></returns>
        public bool ExecuteNonQuery(string _CommandText, CommandType _Commandtype)
        {
            CDataAccess objDataAccessHelper = new CDataAccess();
            return objDataAccessHelper.ExecuteNonQuery(_CommandText, CommandType.Text);

        }

        /// <summary>
        /// Deepak Balhara:19-07-2016
        /// </summary>
        /// <param name="propertyId"></param>
        /// <param name="procedure"></param>
        /// <returns></returns>
        public DataTable AgentImagesDataTable(int propertyId, string procedure)
        {
            try
            {
                CDataAccess objDataAccessHelper = new CDataAccess();
                SqlParameter[] objParameter = new SqlParameter[1];
                objParameter[0] = new SqlParameter("@pUID", SqlDbType.Int); objParameter[0].Value = propertyId;
                return objDataAccessHelper.GetDataTable(procedure, objParameter);
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// Deepak Balhara:19-07-2016
        /// </summary>
        /// <param name="_subjectEmail"></param>
        /// <param name="_toEmailId"></param>
        /// <param name="_fromEmailID"></param>
        /// <param name="_fromEmailtoShow"></param>
        /// <param name="_bodyEmail"></param>
        /// <param name="_hostEmail"></param>
        /// <param name="_authorEmailId"></param>
        /// <param name="_authorPassword"></param>
        /// <returns></returns>
        public string SendMail(string _subjectEmail, string _toEmailId, string _fromEmailID, string _fromEmailtoShow, string _bodyEmail, string _hostEmail, string _authorEmailId, string _authorPassword)
        {

            string strFileName = string.Empty;
            MailMessage mail = new MailMessage();
            try
            {
                mail.To.Add(_toEmailId.Trim());
                mail.From = new MailAddress(_fromEmailID, _fromEmailtoShow);
                mail.Subject = _subjectEmail;
                mail.Body = _bodyEmail;
                // jai mata di                
                mail.IsBodyHtml = true;
                SmtpClient smtp = new SmtpClient();
                smtp.Host = _hostEmail;
                //smtp.EnableSsl = true;
                smtp.Credentials = new System.Net.NetworkCredential
                      (_authorEmailId, _authorPassword);
                smtp.Send(mail);
                return "Sent Successfully!";

            }
            catch (Exception)
            {

                return "Sorry for Inconvenience!";

            }
            finally
            {
                mail.Dispose();
            }

        }

        #endregion
    }
    #endregion
}
