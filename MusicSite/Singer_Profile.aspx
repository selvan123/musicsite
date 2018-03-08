<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Singer_Profile.aspx.cs"
    Inherits="MusicSite.Singer_Profile" MasterPageFile="~/Site1.Master" MaintainScrollPositionOnPostback="true" %>

<asp:Content ID="Content1" runat="server" ContentPlaceHolderID="head">
</asp:Content>
<asp:Content ID="Content2" runat="server" ContentPlaceHolderID="ContentPlaceHolder1">
    <asp:UpdatePanel ID="UpdatePanel1" runat="server" UpdateMode="Conditional" RenderMode="Inline"
        ChildrenAsTriggers="true">
        <ContentTemplate>
            <br />
            <div class="container">
                <div class="row grid-divider">
                    <div class="col-sm-4">
                        <asp:Label ID="lblsingername" CssClass="btn btn-primary" runat="server"></asp:Label>
                    </div>
                    <div class="col-sm-8">
                        <asp:Label ID="Label1" runat="server" Text=""></asp:Label>
                    </div>
                </div>
                <div class="row grid-divider">
                    <div class="col-sm-12">
                        <br />
                    </div>
                </div>
                <div class="row grid-divider">
                    <div class="col-sm-8">
                        <asp:GridView ID="gd_songs" runat="server" GridLines="None" AutoGenerateColumns="false"
                            EmptyDataText="No Song Found" AllowPaging="true" PageSize="20" OnPageIndexChanging="GD_Singers_PageIndexChanging">
                            <Columns>
                                <asp:TemplateField HeaderText="Songs" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px"
                                    ItemStyle-Width="200px">
                                    <ItemTemplate>
                                        <asp:LinkButton ID="lnkDownload" Text='<%# Eval("SongName") %>' CommandArgument='<%# Eval("uid") %>'
                                            runat="server" OnClick="DownloadFile"></asp:LinkButton>
                                    </ItemTemplate>
                                </asp:TemplateField>
                                <asp:BoundField HeaderStyle-Font-Size="17px" ItemStyle-Font-Bold="true" HeaderStyle-Height="40px"
                                    HeaderText="Downloads" DataField="DownloadCount" />
                            </Columns>
                        </asp:GridView>
                    </div>
                    <div class="col-sm-4">
                        <asp:GridView ID="gd_realtedsingers" runat="server" GridLines="None" AutoGenerateColumns="false"
                            EmptyDataText="No Song Found">
                            <Columns>
                                <asp:TemplateField HeaderText="Related Singers" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px">
                                    <ItemTemplate>
                                        <asp:LinkButton ID="lnkRedirect" Text='<%# Eval("SingerName") %>' CommandArgument='<%# Eval("uid") %>'
                                            runat="server" OnClick="redirectMe"></asp:LinkButton>
                                    </ItemTemplate>
                                </asp:TemplateField>
                            </Columns>
                        </asp:GridView>
                    </div>
                </div>
            </div>
            <br />
        </ContentTemplate>
    </asp:UpdatePanel>
</asp:Content>
