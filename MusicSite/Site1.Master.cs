using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace MusicSite
{
    public partial class Site1 : System.Web.UI.MasterPage
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                try
                {

                }
                catch (Exception ex)
                {

                }


            }
        }

        protected void BtnSubmit_Click(object sender, EventArgs e)
        {
            try
            {
                ScriptManager.RegisterClientScriptBlock(Page, Page.GetType(), "Exception", "alert('Your Message is sent');", true);
            }
            catch (Exception ex)
            {

            }
            finally
            {
                TxtEmail.Text = "";
                TxtLName.Text = "";
                TxtMessage.Text = "";
                TxtSubject.Text = "";
            }
        }
    }
}