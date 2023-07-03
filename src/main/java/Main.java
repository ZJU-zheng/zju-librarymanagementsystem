import utils.ConnectConfig;
import utils.DatabaseConnector;
import queries.*;//
import entities.*;//
import entities.Card.CardType;
import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.locks.Condition;
import java.util.logging.Logger;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            // parse connection config from "resources/application.yaml"
            ConnectConfig conf = new ConnectConfig();
            log.info("Success to parse connect config. " + conf.toString());
            // connect to database
            DatabaseConnector connector = new DatabaseConnector(conf);
            boolean connStatus = connector.connect();
            if (!connStatus) {
                log.severe("Failed to connect database.");
                System.exit(1);
            }
            /*do somethings*/
            LibraryManagementSystemImpl impl = new LibraryManagementSystemImpl(connector);
            Scanner temp = new Scanner(System.in);
            while(true){
                System.out.println("<<<<<<<<<>>>>>>>>>");
                System.out.println("欢迎来到图书管理系统");
                System.out.println("输入1以存储一本新书");
                System.out.println("输入2以更改书的库存");
                System.out.println("输入3以移去某书");
                System.out.println("输入4以更改书的信息");
                System.out.println("输入5以查询所需要的书");
                System.out.println("输入6以借书");
                System.out.println("输入7以还书");
                System.out.println("输入8以查询借书历史记录");
                System.out.println("输入9以注册借阅卡");
                System.out.println("输入10以移去借阅卡");
                System.out.println("输入11以查看所有借阅卡信息");
                System.out.println("输入12以批量入库图书");
                System.out.println("输入0以退出");
                int i;
                i = temp.nextInt();
                temp.nextLine();
                if(i!=5){
                    System.out.println("你需要管理员权限,请输入密码");
                    int password = temp.nextInt();
                    temp.nextLine();
                    if(password==123){
                    }
                    else
                        continue;
                }
                if(i==0){
                    break;
                }
                else if(i==1){
                    System.out.println("请输入书的类别");
                    String category = temp.nextLine();
                    System.out.println("请输入书的标题");
                    String title = temp.nextLine();
                    System.out.println("请输入书的出版社");
                    String press = temp.nextLine();
                    System.out.println("请输入书的发行日期");
                    int publishYear = temp.nextInt();
                    temp.nextLine();
                    System.out.println("请输入书的作者");
                    String author = temp.nextLine();
                    System.out.println("请输入书的价格");
                    double price = temp.nextDouble();
                    System.out.println("请输入书的库存");
                    int stock = temp.nextInt();
                    Book book = new Book(category, title, press, publishYear, author, price, stock);
                    impl.storeBook(book);
                }
                else if(i==2){
                    System.out.println("请输入书的ID");
                    int bookId = temp.nextInt();
                    System.out.println("请输入该书库存的更改量");
                    int deltaStock = temp.nextInt();;
                    impl.incBookStock(bookId, deltaStock);
                }
                else if(i==3){
                    System.out.println("请输入书的ID");
                    int bookId = temp.nextInt();
                    impl.removeBook(bookId);
                }
                else if(i==4){
                    System.out.println("请输入要修改的书的ID");
                    int bookID = temp.nextInt();
                    temp.nextLine();
                    System.out.println("请输入修改后书的类别");
                    String category = temp.nextLine();
                    System.out.println("请输入修改后书的标题");
                    String title = temp.nextLine();
                    System.out.println("请输入修改后书的出版社");
                    String press = temp.nextLine();
                    System.out.println("请输入修改后书的发行日期");
                    int publishYear = temp.nextInt();
                    temp.nextLine();
                    System.out.println("请输入修改后书的作者");
                    String author = temp.nextLine();
                    System.out.println("请输入修改后书的价格");
                    double price = temp.nextDouble();
                    Book book = new Book(category, title, press, publishYear, author, price,0);
                    book.setBookId(bookID);
                    impl.modifyBookInfo(book);
                }
                else if(i==5){
                    BookQueryConditions conditions = new BookQueryConditions();
                    System.out.println("请输入要找的书的类别(若没有请直接回车)");
                    String category = temp.nextLine();
                    System.out.println("请输入要找的书的标题(若没有请直接回车)");
                    String title = temp.nextLine();
                    System.out.println("请输入要找的书的出版社(若没有请直接回车)");
                    String press = temp.nextLine();
                    System.out.println("请输入要找的书的最远可能出版年份(若没有请输入-1)");
                    Integer minPublishYear = temp.nextInt();
                    System.out.println("请输入要找的书的最近可能出版年份(若没有请输入9999)");
                    Integer maxPublishYear = temp.nextInt();
                    temp.nextLine();
                    System.out.println("请输入要找的书的作者(若没有请直接回车)");
                    String author = temp.nextLine();
                    System.out.println("请输入要找的书的最低可能价格(若没有请输入-1)");
                    Double minPrice = temp.nextDouble();
                    System.out.println("请输入要找的书的最高可能价格(若没有请输入9999)");
                    Double maxPrice = temp.nextDouble();
                    Book.SortColumn sortBy = Book.SortColumn.BOOK_ID;
                    System.out.println("按照书的ID排序请输入1,按照书的种类排序请输入2,按照书的标题排序请输入3,按照书的出版社排序请输入4,按照书的发行年份排序请输入5,按照书的作者排序请输入6,按照书的价格排序请输入7,按照书的库存排序请输入8");
                    int j = temp.nextInt();
                    if(j==2){
                        sortBy = Book.SortColumn.CATEGORY;
                    }
                    else if(j==3){
                        sortBy = Book.SortColumn.TITLE;
                    }
                    else if(j==4){
                        sortBy = Book.SortColumn.PRESS;
                    }
                    else if(j==5){
                        sortBy = Book.SortColumn.PUBLISH_YEAR;
                    }
                    else if(j==6){
                        sortBy = Book.SortColumn.AUTHOR;
                    }
                    else if(j==7){
                        sortBy = Book.SortColumn.PRICE;
                    }
                    else if(j==8){
                        sortBy = Book.SortColumn.STOCK;
                    }
                    SortOrder sortOrder = SortOrder.ASC;
                    System.out.println("按照升序排序请输入1,降序排序请输入2");
                    j = temp.nextInt();
                    if(j==2){
                        sortOrder = SortOrder.DESC;
                    }
                    if(category!="")
                        conditions.setCategory(category);
                    if(title!="")
                        conditions.setTitle(title);
                    if(press!="")
                        conditions.setPress(press);
                    conditions.setMinPublishYear(minPublishYear);
                    conditions.setMaxPublishYear(maxPublishYear);
                    if(author!="")
                        conditions.setAuthor(author);
                    conditions.setMinPrice(minPrice);
                    conditions.setMaxPrice(maxPrice);
                    conditions.setSortBy(sortBy);
                    conditions.setSortOrder(sortOrder);
                    ApiResult apiresult = impl.queryBook(conditions);
                    BookQueryResults bookqueryresults = (BookQueryResults)apiresult.payload;
                    for(j=0;j<bookqueryresults.getCount();j++){
                        System.out.println(bookqueryresults.getResults().get(j));
                    }
                    if(bookqueryresults.getCount()==0){
                        System.out.println("无");
                    }
                }
                else if(i==6){
                    System.out.println("请输入要借的书的ID");
                    int bookId = temp.nextInt();
                    System.out.println("请输入用来借书的卡的ID");
                    int cardId = temp.nextInt();
                    System.out.println("请输入借书时间");
                    long borrowtime = temp.nextLong();
                    Borrow borrow = new Borrow(bookId, cardId);
                    borrow.setBorrowTime(borrowtime);
                    impl.borrowBook(borrow);
                }
                else if(i==7){
                    System.out.println("请输入要还的书的ID");
                    int bookId = temp.nextInt();
                    System.out.println("请输入用来还书的卡的ID");
                    int cardId = temp.nextInt();
                    Borrow borrow = new Borrow(bookId, cardId);
                    System.out.println("请输入借书时间");
                    borrow.setBorrowTime(temp.nextLong());
                    System.out.println("请输入还书时间");
                    borrow.setReturnTime(temp.nextLong());
                    impl.returnBook(borrow);
                }
                else if(i==8){
                    System.out.println("请输入要查询记录的卡的ID");
                    int cardId = temp.nextInt();
                    ApiResult apiresult = impl.showBorrowHistory(cardId);
                    BorrowHistories borrowhistories = (BorrowHistories)apiresult.payload;
                    int j;
                    for(j=0;j<borrowhistories.getCount();j++){
                        System.out.println(borrowhistories.getItems().get(j));
                    }
                    if(borrowhistories.getCount()==0){
                        System.out.println("此ID无借书记录");
                    }
                }
                else if(i==9){
                    System.out.println("请输入要注册的卡的姓名");
                    String name = temp.nextLine();
                    System.out.println("请输入要注册的卡的部门");
                    String department = temp.nextLine();
                    System.out.println("请输入要注册的卡的类型(1为学生,2为教师)");
                    int j = temp.nextInt();
                    CardType type = CardType.Student;
                    if(j==2){
                        type = CardType.Teacher;
                    }
                    Card card = new Card(0, name, department, type);
                    impl.registerCard(card);
                }
                else if(i==10){
                    System.out.println("请输入要移除的卡的ID");
                    int cardId = temp.nextInt();
                    impl.removeCard(cardId);
                }
                else if(i==11){
                    ApiResult apiresult = impl.showCards();
                    CardList cardlist = (CardList)apiresult.payload;
                    for (int j = 0; j < cardlist.getCount(); j++) {
                        System.out.println(cardlist.getCards().get(j).toString());
                    }
                }
                else if(i==12){
                    List<Book> books = new ArrayList<Book>();
                    System.out.println("请输入文件名称");
                    String filename = temp.nextLine();
                    try {
                        BufferedReader filein = new BufferedReader(new FileReader(filename));
                        String str;
                        String category = null;
                        String title = null;
                        String press = null;
                        int publishYear = 0;
                        String author = null;
                        Double price = 0.0;
                        int stock = 0;
                        int count = 0;
                        while ((str = filein.readLine()) != null) {
                            if(count%7==0)
                                category = str;
                            else if(count%7==1)
                                title = str;
                            else if(count%7==2)
                                press = str;
                            else if(count%7==3)
                                publishYear = Integer.parseInt(str);
                            else if(count%7==4)
                                author = str;
                            else if(count%7==5)
                                price = Double.parseDouble(str);
                            else if(count%7==6)
                                stock = Integer.parseInt(str);
                            count++;
                            if((count%7==0)&&(count!=0)){
                                Book book = new Book(category, title, press, publishYear, author, price, stock);
                                books.add(book);
                            }
                        }
                        filein.close();
                    } catch (IOException e) {
                    }
                    impl.storeBook(books);
                }
            }
            temp.close();
            // release database connection handler
            if (connector.release()) {
                log.info("Success to release connection.");
            } else {
                log.warning("Failed to release connection.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}