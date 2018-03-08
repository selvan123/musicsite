using System;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using BusinessLogicLayer;
using System.Data;
namespace MusicSite.Admin
{
    public partial class Login : System.Web.UI.Page
    {
        UserHelper ObjUser;
        DataTable DT;

        string _Login = string.Empty;
        string _Password = string.Empty;

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                try
                {
                    Clear(Page);
                    Session["Login"] = null;
                    UserName.Text = "";
                    UserName.Focus();
                }
                catch (Exception ex)
                {
                    ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
                }
            }
        }



        protected void Login_Auth()
        {
            try
            {
                ObjUser = new UserHelper();
                DT = new DataTable();

                _Login = UserName.Text.Trim();
                _Password = Password.Text.Trim();

                if ((_Login != "") && (_Password != ""))
                {
                    HttpCookie UserLogin = new HttpCookie("LOGIN");

                    UserLogin.Value = _Login;
                    UserLogin.Value = _Password;

                    Response.Cookies.Add(UserLogin);

                    Response.Cookies["UserLogin"].Value = _Login;
                    Response.Cookies["UserLogin"].Value = _Password;

                    DT = ObjUser.LoginUser(_Login, _Password);

                    if (DT.Rows.Count > 0)
                    {
                        Session["Login"] = _Login;

                        //Login1.FailureText = "";
                        if (RememberMe.Checked == true)
                        {  //Check if the browser support cookies 

                            if (Request.Browser.Cookies)
                            {
                                //Check if the cookie with name PBLOGIN exist on user's machine 
                                if (Request.Cookies["LOGIN"] == null)
                                {
                                    //Create a cookie with expiry of 30 days 

                                    Response.Cookies["LOGIN"].Expires = DateTime.Now.AddDays(30);

                                    //Write username to the cookie 

                                    Response.Cookies["UserLogin"].Value = null;
                                    Response.Cookies["UserLogin"].Value = null;
                                    Response.Cookies["UserLogin"].Value = null;
                                    Response.Cookies["UserLogin"].Value = null;

                                    Response.Cookies.Clear();
                                }
                                //If the cookie already exist then write the user name and password on the cookie 
                                else
                                {
                                    Response.Cookies["UserLogin"].Value = _Login;
                                    Response.Cookies["UserLogin"].Value = _Password;
                                }
                            }
                            else
                            {
                                Response.Cookies.Clear();
                                Response.Cookies["UserLogin"].Value = null;
                                Response.Cookies["UserLogin"].Value = null;
                                Response.Cookies["UserLogin"].Value = null;
                                Response.Cookies["UserLogin"].Value = null;
                            }
                        }
                        else
                        {
                            Response.Cookies.Clear();
                            Response.Cookies["UserLogin"].Value = null;
                            Response.Cookies["UserLogin"].Value = null;
                            Response.Cookies["UserLogin"].Value = null;
                            Response.Cookies["UserLogin"].Value = null;
                        }
                        Response.Redirect("music_upload.aspx");
                        Clear(Page);
                    }
                    else
                    {
                        ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('Your login attempt was not successful, Please try again');", true);
                    }
                }
                else
                {
                    ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('Fill a User Name ID & Password');", true);
                }
            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('" + ex.Message + "');", true);
            }
        }

        public void Clear(Control c)
        {
            foreach (Control c1 in c.Controls)
            {
                if (c1.GetType() == typeof(TextBox))
                {
                    ((TextBox)c1).Text = "";
                }
                if (c1.HasControls())
                {
                    Clear(c1);
                }
            }
        }

        protected void LoginButton_OnCommand(Object sender, CommandEventArgs e)
        {
            try
            {
                DT = new DataTable();
                ObjUser = new UserHelper();

                if (e.CommandName.Equals("Login"))
                {
                    Login_Auth();
                }
                else
                {
                    ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('Your login attempt was not successful, Please try again');", true);
                }
            }
            catch (Exception ex)
            {
                ScriptManager.RegisterClientScriptBlock(this.Page, this.Page.GetType(), "alertscript", ex.Message.ToString(), true);
            }
        }
    }
}

