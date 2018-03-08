<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Index.aspx.cs" Inherits="MusicSite.Index"
    MasterPageFile="~/Site1.Master" MaintainScrollPositionOnPostback="true" %>

<asp:Content runat="server" ContentPlaceHolderID="head">
</asp:Content>
<asp:Content runat="server" ContentPlaceHolderID="ContentPlaceHolder1">
    <asp:UpdatePanel ID="UpdatePanel1" runat="server">
        <ContentTemplate>
            <section class="banner" role="banner">         
        <div class="container">
            <div class="col-md-10 col-md-offset-1">
                <div class="banner-text text-center">
                    <a href="all_songs.aspx"><h1>!! Easiest Way to download a song with 1 click !!</h1>
                    </a>
                    <h3> <strong>Free Music</strong> gives you instant access to millions of songs – from old 
                    favorites to the latest hits. Just hit play to stream anything you like.</h3>
                   </div>
            </div>
        </div>
    </section>
            <br />
            <br />
            <section id="teams" class="section teams">
        <div class="container">
            <div class="row">
                <div class="col-md-3 col-sm-6">
                    <div class="person">
                        <img src="images/premium-img1.jpg" alt="" class="img-responsive">
                        <div class="person-content" style="text-align:center">
                            <h4><strong>Listen everywhere</strong></h4>
                            <p>Spotify works on your computer, mobile, tablet and TV.</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="person">
                        <img src="images/premium-img2.jpg" alt="" class="img-responsive">
                        <div class="person-content" style="text-align:center">
                            <h4><strong>Unlimited, ad-free music</strong></h4>
                              <p>No ads. No interruptions. Just music.</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="person">
                        <img src="images/premium-img3.jpg" alt="" class="img-responsive">
                        <div class="person-content" style="text-align:center">
                            <h4><strong>Download music & listen offline</strong></h4>
                            <p>Keep playing, even when you don't have a connection.</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="person">
                        <img src="images/premium-img4.jpg" alt="" class="img-responsive">
                        <div class="person-content" style="text-align:center" >
                            <h4><strong>Free sounds better</strong></h4>                            
                            <p>Get ready for incredible sound quality.</p>
                        </div>                      
                    </div>
                </div>
            </div>
        </div>
    </section>
            <div class="container-fluid">
                <%--<div class="container">
                    <div class="row">
                        <div class="col-lg-2 col-md-2">
                            <ul class="list-unstyled clear-margins">
                                <li class="widget-container widget_nav_menu">
                                    <asp:GridView ID="GridView1" runat="server" GridLines="None" AutoGenerateColumns="false"
                                        EmptyDataText="No files uploaded" CssClass="abc">
                                        <Columns>
                                            <asp:TemplateField HeaderText="Recent Uploads" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px">
                                                <ItemTemplate>
                                                    <asp:LinkButton ID="lnkDownload" Text='<%# Eval("SongName") %>' CommandArgument='<%# Eval("UID") %>'
                                                        runat="server" OnClick="DownloadFile"></asp:LinkButton>
                                                </ItemTemplate>
                                            </asp:TemplateField>
                                        </Columns>
                                    </asp:GridView>
                                </li>
                            </ul>
                        </div>
                        <div class="col-lg-2 col-md-2">
                            <ul class="list-unstyled clear-margins">
                                <li class="widget-container widget_nav_menu">
                                    <asp:GridView ID="GridView2" runat="server" GridLines="None" AutoGenerateColumns="false"
                                        EmptyDataText="No files uploaded" CssClass="abc">
                                        <Columns>
                                            <asp:TemplateField HeaderText="Latest Hits" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px">
                                                <ItemTemplate>
                                                    <asp:LinkButton ID="lnkDownload" Text='<%# Eval("Name") %>' CommandArgument='<%# Eval("Name") %>'
                                                        runat="server"></asp:LinkButton>
                                                </ItemTemplate>
                                            </asp:TemplateField>
                                        </Columns>
                                    </asp:GridView>
                                </li>
                            </ul>
                        </div>
                        <div class="col-lg-2 col-md-2">
                            <ul class="list-unstyled clear-margins">
                                <li class="widget-container widget_nav_menu">
                                    <asp:GridView ID="GridView3" runat="server" GridLines="None" AutoGenerateColumns="false"
                                        EmptyDataText="No files uploaded" CssClass="abc">
                                        <Columns>
                                            <asp:TemplateField HeaderText="Hip Hops" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px">
                                                <ItemTemplate>
                                                    <asp:LinkButton ID="lnkDownload" Text='<%# Eval("Name") %>' CommandArgument='<%# Eval("Name") %>'
                                                        runat="server"></asp:LinkButton>
                                                </ItemTemplate>
                                            </asp:TemplateField>
                                        </Columns>
                                    </asp:GridView>
                                </li>
                            </ul>
                        </div>
                        <div class="col-lg-2 col-md-2">
                            <ul class="list-unstyled clear-margins">
                                <li class="widget-container widget_nav_menu">
                                    <asp:GridView ID="GridView4" runat="server" GridLines="None" AutoGenerateColumns="false"
                                        EmptyDataText="No files uploaded" CssClass="abc">
                                        <Columns>
                                            <asp:TemplateField HeaderText="Top Tracks" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px">
                                                <ItemTemplate>
                                                    <asp:LinkButton ID="lnkDownload" Text='<%# Eval("Name") %>' CommandArgument='<%# Eval("Name") %>'
                                                        runat="server"></asp:LinkButton>
                                                </ItemTemplate>
                                            </asp:TemplateField>
                                        </Columns>
                                    </asp:GridView>
                                </li>
                            </ul>
                        </div>
                        <div class="col-lg-2 col-md-2">
                            <ul class="list-unstyled clear-margins">
                                <li class="widget-container widget_nav_menu">
                                    <asp:GridView ID="gd_singer" runat="server" GridLines="None" AutoGenerateColumns="false"
                                        EmptyDataText="No files uploaded" CssClass="abc">
                                        <Columns>
                                            <asp:TemplateField HeaderText="Top Singers" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px">
                                                <ItemTemplate>
                                                    <asp:LinkButton ID="lnkDownload" Text='<%# Eval("SingerName") %>' CommandArgument='<%# Eval("SingerName") %>'
                                                        runat="server"></asp:LinkButton>
                                                </ItemTemplate>
                                            </asp:TemplateField>
                                        </Columns>
                                    </asp:GridView>
                                </li>
                            </ul>
                        </div>
                        <div class="col-lg-2 col-md-2">
                            <ul class="list-unstyled clear-margins">
                                <li class="widget-container widget_nav_menu">
                                    <asp:GridView ID="GridView6" runat="server" GridLines="None" AutoGenerateColumns="false"
                                        EmptyDataText="No files uploaded" CssClass="abc">
                                        <Columns>
                                            <asp:TemplateField HeaderText="Local Singers" HeaderStyle-Font-Size="17px" HeaderStyle-Height="40px">
                                                <ItemTemplate>
                                                    <asp:LinkButton ID="lnkDownload" Text='<%# Eval("Name") %>' CommandArgument='<%# Eval("Name") %>'
                                                        runat="server"></asp:LinkButton>
                                                </ItemTemplate>
                                            </asp:TemplateField>
                                        </Columns>
                                    </asp:GridView>
                                </li>
                            </ul>
                        </div>
                    </div>
                </div>--%>
            </div>
        </ContentTemplate>
    </asp:UpdatePanel>
</asp:Content>
