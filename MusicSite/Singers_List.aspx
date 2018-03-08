<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Singers_List.aspx.cs" Inherits="MusicSite.Singers_List"
    MasterPageFile="~/Site1.Master" MaintainScrollPositionOnPostback="true" %>

<asp:Content ID="Content1" runat="server" ContentPlaceHolderID="head">
</asp:Content>
<asp:Content ID="Content2" runat="server" ContentPlaceHolderID="ContentPlaceHolder1">
    <asp:UpdatePanel ID="UpdatePanel1" runat="server">
        <ContentTemplate>
            <br />
            <asp:Label ID="Label1" runat="server" Text=""></asp:Label>
            <div class="container">
                <div class="row">
                    <div class="col-sm-4">
                        <asp:GridView ID="GD_Singers" runat="server" GridLines="None" AutoGenerateColumns="false"
                            EmptyDataText="No files uploaded" CssClass="abc" AllowPaging="true" PageSize="20"
                            OnPageIndexChanging="GD_Singers_PageIndexChanging">
                            <Columns>
                                <asp:TemplateField HeaderText="Popular Artists" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px"
                                    ItemStyle-Width="200px">
                                    <ItemTemplate>
                                        <asp:LinkButton ID="lnkRedirect" Text='<%# Eval("SingerName") %>' CommandArgument='<%# Eval("UID") %>'
                                            runat="server" OnClick="redirectMe"></asp:LinkButton>
                                    </ItemTemplate>
                                </asp:TemplateField>
                                <asp:BoundField HeaderStyle-Font-Size="17px" ItemStyle-Font-Bold="true" HeaderStyle-Height="40px"
                                    HeaderText="Popularity" DataField="SingerRating" />
                            </Columns>
                        </asp:GridView>
                    </div>
                    <div class="col-sm-8">
                        <center>
                            <asp:Label ID="Label2" CssClass="btn btn-primary" runat="server" Text="All Singer"></asp:Label>

                        </center>
                    </div>
                </div>
            </div>
            <br />
        </ContentTemplate>
    </asp:UpdatePanel>
</asp:Content>
