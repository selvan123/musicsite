using System;
using System.Web;
using System.Web.UI;
using System.IO;
using System.Web.UI.WebControls;
using BusinessLogicLayer;
using System.Data;


namespace MusicSite
{
    public partial class Index : System.Web.UI.Page
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
                    ScriptManager scriptManager = ScriptManager.GetCurrent(this.Page);
                    //scriptManager.RegisterPostBackControl(this.gd_recentuploads);
                    //scriptManager.RegisterPostBackControl(this.gd_latesthits);
                    //scriptManager.RegisterPostBackControl(this.gd_topdownloads);

                    bindRecentUploads();
                    bindLatestHits();
                    bindTopDownloads();
                    bindHollywoodSingers();
                    bindBollywoodSingers();
                    bindPunjabiSingers();

                }
                catch (Exception ex)
                {
                    ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
                }
            }
        }


        protected void bindRecentUploads()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("get_recent_uploads", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {

                    //gd_recentuploads.DataSource = DT;
                    //gd_recentuploads.DataBind();

                }

            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        protected void bindLatestHits()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("get_latest_hits", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {

                    //gd_latesthits.DataSource = DT;
                    //gd_latesthits.DataBind();

                }

            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        protected void bindTopDownloads()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("get_top_downloads", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {

                    //gd_topdownloads.DataSource = DT;
                    //gd_topdownloads.DataBind();

                }

            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        protected void bindHollywoodSingers()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("get_top_hollywood_singers", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {

                    //gd_hollywoodsingers.DataSource = DT;
                    //gd_hollywoodsingers.DataBind();

                }

            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        protected void bindBollywoodSingers()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("get_top_bollywood_singers", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {

                    //gd_bollywoodsingers.DataSource = DT;
                    //gd_bollywoodsingers.DataBind();

                }

            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        protected void bindPunjabiSingers()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("get_top_punjabi_singers", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {

                    //gd_punjabisingers.DataSource = DT;
                    //gd_punjabisingers.DataBind();

                }

            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        protected void DownloadFile(object sender, EventArgs e)
        {
            try
            {
                objcommon = new clsCommonHelper();
                GridViewRow lnk = ((LinkButton)sender).NamingContainer as GridViewRow;
                LinkButton lbl = (LinkButton)lnk.FindControl("lnkDownload");

                string abc = lbl.CommandArgument.ToString();

                DT = objcommon.FillDataTable("GetSongPath", abc);
                if (DT.Rows.Count > 0)
                {
                    string filepath = DT.Rows[0]["SongName"] + "";


                    Response.Clear();
                    Response.AppendHeader("Content-Disposition", "attachment; filename=" + filepath);
                    string abc1 = Server.MapPath("/Data/" + filepath.ToString());
                    Response.TransmitFile(abc1);
                    Response.Flush();
                    Response.End();

                }
            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        protected void redirectMe(object sender, EventArgs e)
        {
            try
            {
                LinkButton btn = (LinkButton)(sender);
                singerId = btn.CommandArgument;

                searchedsinger = btn.Text.ToString();

                string singerInfo = "singerprofile.aspx?singerId=" + singerId + "&searchedsinger=" + searchedsinger;
                Response.Redirect(singerInfo, false);
            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }

        }

    }
}
