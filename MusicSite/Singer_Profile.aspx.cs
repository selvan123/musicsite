using System;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using BusinessLogicLayer;
using System.Data;
using System.Drawing;


namespace MusicSite
{
    public partial class Singer_Profile : System.Web.UI.Page
    {
        clsCommonHelper objcommon;
        DataTable DT;
        private static string singerId;
        private string singerId1;
        private string searchedsinger1;

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                try
                {
                    singerId = Request.QueryString["singerId"];
                    lblsingername.Text = Request.QueryString["searchedsinger"];
                    bindSongGrid(singerId);
                    bindRealtedSingers(singerId);
                    ScriptManager scriptManager = ScriptManager.GetCurrent(this.Page);
                    scriptManager.RegisterPostBackControl(this.gd_songs);

                }
                catch (Exception ex)
                {
                    Label1.Text = ex.Message.ToString();
                    Label1.ForeColor = Color.Red;
                }
            }
        }


        protected void DownloadFile(object sender, EventArgs e)
        {
            try
            {
                objcommon = new clsCommonHelper();
                GridViewRow lnk = ((LinkButton)sender).NamingContainer as GridViewRow;
                LinkButton lbl = (LinkButton)lnk.FindControl("lnkDownload");

                string SongId = lbl.CommandArgument.ToString();

                DT = objcommon.FillDataTable("Get_Song_Path", SongId);
                if (DT.Rows.Count > 0)
                {
                    string filepath = DT.Rows[0]["SongPath"] + "";


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
                Label1.Text = ex.Message.ToString();
                Label1.ForeColor = Color.Red;
            }
        }

        protected void GD_Singers_PageIndexChanging(object sender, GridViewPageEventArgs e)
        {
            try
            {
                gd_songs.PageIndex = e.NewPageIndex;
                bindSongGrid(singerId);
            }
            catch (Exception ex)
            {
                Label1.Text = ex.Message.ToString();
                Label1.ForeColor = Color.Red;
            }
        }

        protected void bindSongGrid(string singerId)
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("Get_Song_Singer_Wise", singerId.ToString().Trim());
                if (DT.Rows.Count > 0)
                {

                    gd_songs.DataSource = DT;
                    gd_songs.DataBind();

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
                singerId1 = btn.CommandArgument;

                searchedsinger1 = btn.Text.ToString();

                string singerInfo = "singerprofile.aspx?singerId1=" + singerId1 + "&searchedsinger1=" + searchedsinger1;
                Response.Redirect(singerInfo, false);
            }
            catch (Exception ex)
            {
                Label1.Text = ex.Message.ToString();
                Label1.ForeColor = Color.Red;
            }

        }


        protected void bindRealtedSingers(string singerId)
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();

                DT = objcommon.FillDataTable("Get_related_singers", singerId);
                if (DT.Rows.Count > 0)
                {

                    gd_realtedsingers.DataSource = DT;
                    gd_realtedsingers.DataBind();

                }

            }
            catch (Exception ex)
            {
                Label1.Text = ex.Message.ToString();
                Label1.ForeColor = Color.Red;
            }
        }

    }
}