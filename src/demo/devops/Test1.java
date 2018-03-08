package demo.devops;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.openqa.selenium.firefox.FirefoxDriver;

public class Test1 {
	public static WebDriver driver;
	@BeforeSuite
	public static void setup(){
		System.setProperty("webdriver.chrome.driver", "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe");
		
		driver=new ChromeDriver();
		driver.manage().window().maximize();
		driver.manage().timeouts().implicitlyWait(100, TimeUnit.SECONDS);
		driver.get("https://s3-us-west-2.amazonaws.com/dotnetdata/elb-dns.txt");
		String url=driver.findElement(By.xpath("//body")).getText();
		driver.get(url);
	}
	
	@Test
	public static void test1(){
		
		String title=driver.getTitle();
		if(title.equalsIgnoreCase("Home")){
			System.out.println("Application is loaded properly");
		}
		else{
			System.out.println("Application is not loaded properly");
			Assert.fail();
		}
	}
}
