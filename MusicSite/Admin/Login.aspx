<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Login.aspx.cs" Inherits="MusicSite.Admin.Login" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<head id="Head1" runat="server">
    <title></title>
</head>
<body>
    <form id="form1" runat="server">
    <div align="center">
    <h2>
        Log In
    </h2>
   
    <table width="38%">
        <tr>
            <td>
                <asp:ValidationSummary ID="LoginUserValidationSummary" runat="server" ShowSummary="false"
                    ShowMessageBox="false" CssClass="failureNotification" ValidationGroup="LoginUserValidationGroup" />
            </td>
        </tr>
        <tr>
            <td>
                <fieldset>
                    <legend>Account Information</legend>
                    <table width="100%">
                        <tr>
                            <td>
                                Username :
                            </td>
                            <td>
                                <asp:TextBox ID="UserName" runat="server" CssClass="textEntry" Width="200px"></asp:TextBox>
                                <asp:RequiredFieldValidator ID="UserNameRequired" runat="server" ControlToValidate="UserName"
                                    CssClass="failureNotification" ErrorMessage="User Name is required." ToolTip="User Name is required."
                                    ValidationGroup="LoginUserValidationGroup">*</asp:RequiredFieldValidator>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Password :
                            </td>
                            <td>
                                <asp:TextBox ID="Password" runat="server" CssClass="passwordEntry" Width="200px"
                                    TextMode="Password"></asp:TextBox>
                                <asp:RequiredFieldValidator ID="PasswordRequired" runat="server" ControlToValidate="Password"
                                    CssClass="failureNotification" ErrorMessage="Password is required." ToolTip="Password is required."
                                    ValidationGroup="LoginUserValidationGroup">*</asp:RequiredFieldValidator>
                            </td>
                        </tr>
                        <tr>
                            <td>
                            </td>
                            <td>
                                <asp:CheckBox ID="RememberMe" runat="server" />
                                <asp:Label ID="RememberMeLabel" runat="server" CssClass="inline">Keep me logged in</asp:Label>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2" align="right">
                                <asp:Button ID="LoginButton" runat="server" CommandName="Login" Text="Log In" OnCommand="LoginButton_OnCommand"
                                    ValidationGroup="LoginUserValidationGroup" />
                            </td>
                        </tr>
                    </table>
                </fieldset>
            </td>
        </tr>
    </table>
    </div>
    </form>
</body>
</html>
