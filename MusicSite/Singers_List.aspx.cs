using System;
using System.Web;
using System.Web.UI;
using System.IO;
using System.Web.UI.WebControls;
using BusinessLogicLayer;
using System.Data;
using System.Drawing;

namespace MusicSite
{
    public partial class Singers_List : System.Web.UI.Page
    {
        clsCommonHelper objcommon;
        DataTable DT;
        public string singerId;
        public string searchedsinger;

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                try
                {
                    bindPopularSingerGrid();

                }
                catch (Exception ex)
                {
                    Label1.Text = ex.Message.ToString();
                    Label1.ForeColor = Color.Red;
                }
            }
        }
        protected void GD_Singers_PageIndexChanging(object sender, GridViewPageEventArgs e)
        {
            GD_Singers.PageIndex = e.NewPageIndex;
            bindPopularSingerGrid();
        }
        protected void bindPopularSingerGrid()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("Get_Popular_Singer", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {

                    GD_Singers.DataSource = DT;
                    GD_Singers.DataBind();

                }

            }
            catch (Exception ex)
            {
                Label1.Text = ex.Message.ToString();
                Label1.ForeColor = Color.Red;
            }
        }

        protected void redirectMe(object sender, EventArgs e)
        {
            try
            {
                LinkButton btn = (LinkButton)(sender);
                singerId = btn.CommandArgument;

                searchedsinger = btn.Text.ToString();

                string singerInfo = "Singer_Profile.aspx?singerId=" + singerId + "&searchedsinger=" + searchedsinger;
                Response.Redirect(singerInfo, false);
            }
            catch (Exception ex)
            {
                Label1.Text = ex.Message.ToString();
                Label1.ForeColor = Color.Red; 
            }

        }
    }
}
