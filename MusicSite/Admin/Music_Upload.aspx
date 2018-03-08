<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Music_Upload.aspx.cs" Inherits="MusicSite.Admin.Music_Upload" %>

<%@ Register Assembly="AjaxControlToolkit" Namespace="AjaxControlToolkit" TagPrefix="cc1" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title>Welcome Admin</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css" />
    <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js"></script>
    <script type="text/javascript" src="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
    <link href="Style/footer.css" rel="stylesheet" type="text/css" />
    <link href="xtyles/chosen.css" rel="stylesheet" type="text/css" />
    <script src="Scripts/jquery.min.js" type="text/javascript"></script>
    <script src="Scripts/chosen.jquery.js" type="text/javascript"></script>
</head>
<body>
    <form id="form1" runat="server">
    <asp:ScriptManager ID="ScriptManager1" runat="server">
    </asp:ScriptManager>
    <asp:UpdatePanel ID="UpdatePanel1" runat="server" UpdateMode="Conditional" RenderMode="Inline"
        ChildrenAsTriggers="true">
        <ContentTemplate>
            <div class="container-fluid">
                <div class="row grid-divider">
                    <div class="col-sm-10">
                        <div class="col-padding">
                            <h3>
                                Admin Arena</h3>
                        </div>
                    </div>
                    <div class="col-sm-2" style="padding-top: 1em">
                        <div class="col-padding">
                            <asp:Button ID="AbtnLogout" runat="server" Text="Logout" CssClass="btn btn-success"
                                OnClick="HeadLogout_Click" />
                        </div>
                    </div>
                </div>
            </div>
            <div>
                <hr />
            </div>
            <div class="container-fluid">
                <div class="row grid-divider">
                    <div class="col-sm-3">
                        <div class="col-padding">
                            <h4>
                                Choose File</h4>
                            <asp:FileUpload ID="FileUpload1" runat="server" />
                            <asp:RequiredFieldValidator ID="RequiredFieldValidator5" ErrorMessage="***" ControlToValidate="FileUpload1"
                                runat="server" ForeColor="Red" ValidationGroup="upload" />
                            <%-- <asp:RegularExpressionValidator ID="RegularExpressionValidator1" ForeColor="Red" ControlToValidate="FileUpload1"
                                ValidationGroup="upload" ValidationExpression="/^(([a-zA-Z]:)|(\\{2}\w+)\$?)(\\(\w[\w].*))+(.mp3|.MP3)$/;"
                                runat="server" ErrorMessage="Upload an audio file"></asp:RegularExpressionValidator>--%>
                            <asp:Label ID="Label1" runat="server" Text="" ForeColor="Red"></asp:Label>
                        </div>
                    </div>
                    <div class="col-sm-3">
                        <div class="col-padding">
                            <h4>
                                Song Name
                            </h4>
                            <asp:TextBox ID="txtsongname" runat="server" CssClass="form-control"></asp:TextBox>
                            <asp:RequiredFieldValidator runat="server" ID="RequiredFieldValidator6" ControlToValidate="txtsongname"
                                ErrorMessage="***" ValidationGroup="upload" ForeColor="red"></asp:RequiredFieldValidator>
                        </div>
                    </div>
                    <div class="col-sm-3">
                        <div class="col-padding">
                            <h4>
                                Song/Singer Type</h4>
                            <asp:DropDownList ID="ddlcategory" runat="server" class="chzn-select" Width="250px"
                                data-placeholder="Choose">
                            </asp:DropDownList>
                            <br />
                            <asp:RequiredFieldValidator runat="server" ID="RequiredFieldValidator2" ControlToValidate="ddlcategory"
                                ErrorMessage="***" ValidationGroup="upload" ForeColor="red" InitialValue="-1"></asp:RequiredFieldValidator>
                        </div>
                    </div>
                    <div class="col-sm-3">
                        <div class="col-padding">
                            <h4>
                                Singer Name</h4>
                            <asp:DropDownList ID="ddlsinger" runat="server" class="chzn-select" Width="250px"
                                data-placeholder="Choose">
                            </asp:DropDownList>
                            <br />
                            <asp:RequiredFieldValidator runat="server" ID="RequiredFieldValidator1" ControlToValidate="ddlsinger"
                                ErrorMessage="***" ValidationGroup="upload" ForeColor="red" InitialValue="-1"></asp:RequiredFieldValidator>
                        </div>
                    </div>
                </div>
            </div>
            <hr />
            <script type="text/javascript">
                $(".chzn-select").chosen(); $(".chzn-select-deselect").chosen({ allow_single_deselect: true });  
            </script>
            <div class="container-fluid" align="center">
                <div class="row grid-divider">
                    <div class="col-sm-4">
                    </div>
                    <div class="col-sm-8">
                        <asp:Button ID="btnUpload" runat="server" Text="Upload" ValidationGroup="upload"
                            OnClick="uploadFile" CssClass="btn btn-success" />
                        <hr />
                    </div>
                </div>
            </div>
            <div class="container-fluid">
                <div class="row grid-divider">
                    <div class="col-sm-4">
                        <h4>
                            Add Singer/Song Type</h4>
                        <hr />
                    </div>
                    <div class="col-sm-8">
                        <h4>
                            Add Singer</h4>
                        <asp:Label ID="lblmsg" runat="server" Text=""></asp:Label>
                    </div>
                </div>
                <div class="row grid-divider">
                    <div class="col-sm-4">
                        <div class="col-padding">
                            <asp:TextBox ID="TxtCategory" runat="server" CssClass="form-control" Width="300px"
                                placeholder="add Singer/Song Type"></asp:TextBox>
                            <asp:RequiredFieldValidator runat="server" ID="RequiredFieldValidator3" ControlToValidate="TxtCategory"
                                ErrorMessage="***" ValidationGroup="category" ForeColor="red"></asp:RequiredFieldValidator>
                            <br />
                            <asp:ImageButton ID="btnsavecategory" ValidationGroup="category" runat="server" CssClass="btn btn-success"
                                OnClick="btnSaveCategory_Click" />
                        </div>
                    </div>
                    <div class="col-sm-8">
                        <div class="col-padding">
                            <asp:TextBox ID="txtsinger" runat="server" CssClass="form-control" Width="300px"
                                placeholder="add singer"></asp:TextBox>
                            <asp:RequiredFieldValidator runat="server" ID="RequiredFieldValidator4" ControlToValidate="txtsinger"
                                ErrorMessage="***" ValidationGroup="singer" ForeColor="red"></asp:RequiredFieldValidator>
                            <br />
                            <asp:DropDownList ID="ddlsingertype" runat="server" class="chzn-select" Width="300px"
                                data-placeholder="Choose">
                            </asp:DropDownList>
                            <asp:RequiredFieldValidator runat="server" ID="RequiredFieldValidator7" ControlToValidate="ddlsingertype"
                                ErrorMessage="***" ValidationGroup="singer" ForeColor="red" InitialValue="-1"></asp:RequiredFieldValidator>
                            <br />
                            <asp:ImageButton ID="btnsavesinger" runat="server" ValidationGroup="singer" CssClass="btn btn-success"
                                OnClick="btnSaveSinger_Click" />
                        </div>
                        <script type="text/javascript">
                            $(".chzn-select").chosen(); $(".chzn-select-deselect").chosen({ allow_single_deselect: true });  
                        </script>
                    </div>
                </div>
            </div>
            <br />
            <hr />
            <br />
            <asp:GridView ID="GridView1" runat="server" CssClass="table table-hover table-bordered"
                AutoGenerateColumns="false" EmptyDataText="No files uploaded">
                <Columns>
                    <asp:BoundField DataField="Text" HeaderText="File Name" />
                    <asp:TemplateField>
                        <ItemTemplate>
                            <asp:LinkButton ID="lnkDownload" Text="Download" CommandArgument='<%# Eval("Value") %>'
                                runat="server" OnClick="DownloadFile"></asp:LinkButton>
                        </ItemTemplate>
                    </asp:TemplateField>
                </Columns>
            </asp:GridView>
        </ContentTemplate>
        <Triggers>
            <asp:PostBackTrigger ControlID="btnUpload" />
            <asp:PostBackTrigger ControlID="btnsavesinger" />
            <asp:PostBackTrigger ControlID="btnsavecategory" />
        </Triggers>
    </asp:UpdatePanel>
    </form>
</body>
</html>
