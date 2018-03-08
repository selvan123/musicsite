using System;
using System.Data;
using DataBaseLayer;
using System.Data.SqlClient;

namespace BusinessLogicLayer
{
    public class SongHelper
    {
        
        private string _SongName;
        private string _TypeID;
        private string _SingerID;
        private string _SongPath;

        
       
        public string SingerID { get { return _SingerID; } set { _SingerID = value; } }
        public string SongName { get { return _SongName; } set { _SongName = value; } }
        public string TypeID { get { return _TypeID; } set { _TypeID = value; } }
        public string SongPath { get { return _SongPath; } set { _SongPath = value; } }
      

        public string saveSong()
        {
            string _result;

            try
            {
                SqlParameter[] objParameter = new SqlParameter[5];

                objParameter[0] = new SqlParameter("@psongname", SqlDbType.NVarChar, 50); objParameter[0].Value = SongName;
                objParameter[1] = new SqlParameter("@psingerID", SqlDbType.NVarChar, 50); objParameter[1].Value = SingerID;
                objParameter[2] = new SqlParameter("@pTypeID", SqlDbType.NVarChar, 50); objParameter[2].Value = TypeID;
                objParameter[3] = new SqlParameter("@psongpath", SqlDbType.NVarChar, 50); objParameter[3].Value = SongPath;
                objParameter[4] = new SqlParameter("@pvcOut", SqlDbType.VarChar, 50); objParameter[4].Direction = ParameterDirection.Output;
                CDataAccess.ExecuteNonQuery(CommandType.StoredProcedure, "Add_Song", objParameter);

                _result = objParameter[4].Value.ToString();

                return _result;

            }
            catch (Exception ex)
            {
                return _result = ex.Message.ToString();
            }

        }

    }
}
