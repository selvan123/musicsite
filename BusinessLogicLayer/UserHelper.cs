using System;
using System.Data;
using DataBaseLayer;
using System.Data.SqlClient;

namespace BusinessLogicLayer
{
    public class UserHelper
    {
        private string _Category;
        private string _Singer;
        private string _SingerTypeID;

        public string Category { get { return _Category; } set { _Category = value; } }
        public string Singer { get { return _Singer; } set { _Singer = value; } }
        public string SingerTypeID { get { return _SingerTypeID; } set { _SingerTypeID = value; } }

        public string saveCategory()
        {
            string _result;

            try
            {
                SqlParameter[] objParameter = new SqlParameter[2];

                objParameter[0] = new SqlParameter("@pType", SqlDbType.NVarChar, 50); objParameter[0].Value = Category;
                objParameter[1] = new SqlParameter("@pvcOut", SqlDbType.VarChar, 50); objParameter[1].Direction = ParameterDirection.Output;
                CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "Add_Type", objParameter);

                _result = objParameter[1].Value.ToString();

                return _result;

            }
            catch (Exception ex)
            {
                return _result = ex.Message.ToString();
            }

        }


        public string saveSinger()
        {
            string _result;

            try
            {
                SqlParameter[] objParameter = new SqlParameter[3];

                objParameter[0] = new SqlParameter("@pSinger", SqlDbType.NVarChar, 50); objParameter[0].Value = Singer;
                objParameter[1] = new SqlParameter("@pSingerTypeId", SqlDbType.NVarChar, 50); objParameter[1].Value = SingerTypeID;
                objParameter[2] = new SqlParameter("@pvcOut", SqlDbType.VarChar, 50); objParameter[2].Direction = ParameterDirection.Output;
                CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "Add_Singers", objParameter);

                _result = objParameter[2].Value.ToString();

                return _result;

            }
            catch (Exception ex)
            {
                return _result = ex.Message.ToString();
            }

        }

        public DataTable LoginUser(string user_id, string user_pwd)
        {
            CDataAccess objDataAccessHelper = new CDataAccess();
            SqlParameter[] objParameter = new SqlParameter[2];
            //int Results;
            objParameter[0] = new SqlParameter("@username", SqlDbType.NVarChar, 50); objParameter[0].Value = user_id;
            objParameter[1] = new SqlParameter("@password", SqlDbType.NVarChar, 50); objParameter[1].Value = user_pwd;
            return objDataAccessHelper.GetDataTable("AdminLogin", objParameter);
        }
    }





}
