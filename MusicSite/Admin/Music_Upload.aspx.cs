using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using BusinessLogicLayer;
using System.Data;
using System.Drawing;

namespace MusicSite.Admin
{
    public partial class Music_Upload : System.Web.UI.Page
    {
        clsCommonHelper objcommon;
        DataTable DT;
        UserHelper objUser;
        SongHelper objsong;


        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                try
                {
                    if (Session["Login"] + "" == "")
                    {
                        Response.Redirect("~/Admin/Login.aspx");
                    }

                    else
                    {
                        bindTypeDdl();
                        bindSingerDdl();
                    }
                }
                catch (Exception ex)
                {
                    lblmsg.Text = ex.Message.ToString();
                    lblmsg.ForeColor = Color.Red;
                }
            }
        }

        protected void bindTypeDdl()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();
                DT = objcommon.FillDataTable("Get_Type", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {
                    objcommon.FillDDL(ddlcategory, DT);
                    objcommon.FillDDL(ddlsingertype, DT);
                }
                else
                {
                    ddlcategory.Items.Insert(0, new ListItem("", ""));
                    ddlcategory.Items.Clear();
                    ddlcategory.Items.Insert(0, new ListItem("Select", ""));
                    ddlsingertype.Items.Insert(0, new ListItem("", ""));
                    ddlsingertype.Items.Clear();
                    ddlsingertype.Items.Insert(0, new ListItem("Select", ""));

                }
            }
            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }

        }

        protected void bindSingerDdl()
        {
            try
            {
                objcommon = new clsCommonHelper();
                DT = new DataTable();
                DT = objcommon.FillDataTable("Get_Singer", CommandType.StoredProcedure);
                if (DT.Rows.Count > 0)
                {
                    objcommon.FillDDL(ddlsinger, DT);
                }
                else
                {
                    ddlsinger.Items.Insert(0, new ListItem("", ""));
                    ddlsinger.Items.Clear();
                    ddlsinger.Items.Insert(0, new ListItem("Select", ""));

                }
            }
            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }
        }

        protected void DownloadFile(object sender, EventArgs e)
        {
            try
            {
                string filePath = (sender as LinkButton).CommandArgument;
                Response.ContentType = ContentType;
                Response.AppendHeader("Content-Disposition", "attachment; filename=" + Path.GetFileName(filePath));
                Response.WriteFile(filePath);
                Response.End();
            }
            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }
        }

        protected void HeadLogout_Click(object sender, EventArgs e)
        {
            try
            {
                Session.Clear();
                Session.RemoveAll();
                Session.Abandon();
                Session["Login"] = null;
                AbtnLogout.Visible = false;
                Response.Redirect("~/Index.aspx");
            }

            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;

            }
        }

        protected void btnSaveCategory_Click(object sender, ImageClickEventArgs e)
        {
            try
            {
                saveCategory();
            }

            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }

        }

        protected void btnSaveSinger_Click(object sender, ImageClickEventArgs e)
        {
            try
            {
                saveSinger();
            }
            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }

        }

        protected void saveCategory()
        {
            try
            {
                objUser = new UserHelper();
                objUser.Category = TxtCategory.Text.Trim();
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + objUser.saveCategory() + "');", true);
                bindTypeDdl();
            }

            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }
            finally
            {
                txtsinger.Text = "";
            }
        }

        protected void saveSinger()
        {
            try
            {
                objUser = new UserHelper();
                objUser.Singer = txtsinger.Text.Trim();
                objUser.SingerTypeID = ddlsingertype.SelectedValue.Trim();
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + objUser.saveSinger() + "');", true);
                bindSingerDdl();
            }

            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }
            finally
            {
                txtsinger.Text = "";
                ddlsingertype.SelectedValue = "-1";
            }
        }

        protected void uploadFile(object sender, EventArgs e)
        {
            try
            {
                if (FileUpload1.HasFile)
                {
                    objsong = new SongHelper();
                    FileUpload1.PostedFile.SaveAs(Server.MapPath("~/Data/") + FileUpload1.FileName);
                    string filename = FileUpload1.FileName;
                    objsong.SongPath = filename;
                    objsong.SingerID = ddlsinger.SelectedValue.Trim();
                    objsong.TypeID = ddlcategory.SelectedValue.Trim();
                    objsong.SongName = txtsongname.Text.Trim();
                    ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + objsong.saveSong() + "');", true);

                }

            }
            catch (Exception ex)
            {
                lblmsg.Text = ex.Message.ToString();
                lblmsg.ForeColor = Color.Red;
            }
            finally
            {
                ddlsinger.SelectedValue = "-1";
                ddlcategory.SelectedValue = "-1";
                txtsongname.Text = "";
            }
        }
    }
}