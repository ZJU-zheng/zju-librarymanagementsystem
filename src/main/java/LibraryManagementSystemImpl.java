import entities.Book;
import entities.Borrow;
import entities.Card;
import entities.Card.CardType;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        Connection conn = connector.getConn();
        try {
            String sql="insert into book(`category`,`title`,`press`,`publish_year`,`author`,`price`,`stock`) values (?,?,?,?,?,?,?);";
            PreparedStatement pstmt = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1,book.getCategory());
            pstmt.setString(2,book.getTitle());
            pstmt.setString(3,book.getPress());
            pstmt.setInt(4,book.getPublishYear());
            pstmt.setString(5,book.getAuthor());
            pstmt.setDouble(6,book.getPrice());
            pstmt.setInt(7, book.getStock());
            if(pstmt.executeUpdate()==1){
                ResultSet rs=pstmt.getGeneratedKeys();
                while(rs.next()){
                    book.setBookId(rs.getInt(1));
                }
                System.out.println("你成功存了"+book.toString());
            }
            else{
                System.out.println("错误:存书失败");
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult incBookStock(int bookId, int deltaStock) {
        Connection conn = connector.getConn();
        int stock = -1;
        try {
            String sql="select stock from book where book_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,bookId);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                stock = rs.getInt(1);
            }
            if(stock==(-1)){
                commit(conn);
                System.out.println("错误:没有书的ID是这个" );
                return new ApiResult(false, "no book with this bookId");
            }
            if((stock+deltaStock)<0){
                commit(conn);
                System.out.println("错误:该操作导致库存小于0" );
                return new ApiResult(false, "stock < 0");
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        try {
            String sql="update book set stock=stock+? where book_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,deltaStock);
            pstmt.setInt(2,bookId);
            pstmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        Connection conn = connector.getConn();
        try {
            int i;
            String sql="insert into book(`category`,`title`,`press`,`publish_year`,`author`,`price`,`stock`) values (?,?,?,?,?,?,?);";
            PreparedStatement pstmt = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
            for(i=0;i<books.size();i++){
                pstmt.setString(1,books.get(i).getCategory());
                pstmt.setString(2,books.get(i).getTitle());
                pstmt.setString(3,books.get(i).getPress());
                pstmt.setInt(4,books.get(i).getPublishYear());
                pstmt.setString(5,books.get(i).getAuthor());
                pstmt.setDouble(6,books.get(i).getPrice());
                pstmt.setInt(7, books.get(i).getStock());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            ResultSet rs=pstmt.getGeneratedKeys();
            i=0;
            while(rs.next()){
                    books.get(i).setBookId(rs.getInt(1));
                    i++;
            }
            System.out.println("you successfully store books");
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult removeBook(int bookId) {
        Connection conn = connector.getConn();
        long borrow_time = -1;
        try {
            String sql="select book_id,return_time from borrow where book_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,bookId);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                borrow_time = rs.getLong(2);
                if(borrow_time==0){
                    commit(conn);
                    System.out.println("错误:有人还在借这本书" );
                    return new ApiResult(false, "someone borrow it,you can not remove!");
                }
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        try {
            String sql="delete from book where book_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,bookId);
            if(pstmt.executeUpdate()!=1){
                System.out.println("错误:没有书的ID是这个");
                return new ApiResult(false, "no such book to remove");
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult modifyBookInfo(Book book) {
        Connection conn = connector.getConn();
        try {
            String sql="update book set category=?,title=?,press=?,publish_year=?,author=?,price=? where book_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,book.getCategory());
            pstmt.setString(2,book.getTitle());
            pstmt.setString(3,book.getPress());
            pstmt.setInt(4,book.getPublishYear());
            pstmt.setString(5,book.getAuthor());
            pstmt.setDouble(6,book.getPrice());
            pstmt.setInt(7,book.getBookId());
            pstmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        Connection conn = connector.getConn();
        List<Book> books = new LinkedList<>();
        int count=1;
        try {
            String sql="select * from book where ";
            if(conditions.getCategory()!=null){
                sql+="category=? and ";
            }
            if(conditions.getTitle()!=null){
                sql+="title like ? and ";
            }
            if(conditions.getPress()!=null){
                sql+="press like ? and ";
            }
            if(conditions.getMinPublishYear()!=null){
                sql+="publish_year >=? and ";
            }
            if(conditions.getMaxPublishYear()!=null){
                sql+="publish_year <=? and ";
            }
            if(conditions.getAuthor()!=null){
                sql+="author like ? and ";
            }
            if(conditions.getMinPrice()!=null){
                sql+="price >=? and ";
            }
            if(conditions.getMaxPrice()!=null){
                sql+="price <=? and ";
            }
            sql+="book_id>=1 order by "+conditions.getSortBy()+" "+conditions.getSortOrder()+" ,book_id ASC;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            if(conditions.getCategory()!=null){
                pstmt.setString(count, conditions.getCategory());
                count++;
            }
            if(conditions.getTitle()!=null){
                pstmt.setString(count, "%"+conditions.getTitle()+"%");
                count++;
            }
            if(conditions.getPress()!=null){
                pstmt.setString(count, "%"+conditions.getPress()+"%");
                count++;
            }
            if(conditions.getMinPublishYear()!=null){
                pstmt.setInt(count, conditions.getMinPublishYear());
                count++;
            }
            if(conditions.getMaxPublishYear()!=null){
                pstmt.setInt(count, conditions.getMaxPublishYear());
                count++;
            }
            if(conditions.getAuthor()!=null){
                pstmt.setString(count, "%"+conditions.getAuthor()+"%");
                count++;
            }
            if(conditions.getMinPrice()!=null){
                pstmt.setDouble(count, conditions.getMinPrice());
                count++;
            }
            if(conditions.getMaxPrice()!=null){
                pstmt.setDouble(count, conditions.getMaxPrice());
                count++;
            }
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                Book book = new Book();
                book.setBookId(rs.getInt(1));
                book.setCategory(rs.getString(2));
                book.setTitle(rs.getString(3));
                book.setPress(rs.getString(4));
                book.setPublishYear(rs.getInt(5));
                book.setAuthor(rs.getString(6));
                book.setPrice(rs.getDouble(7));
                book.setStock(rs.getInt(8));
                books.add(book);
            }
            BookQueryResults bookqueryresults = new BookQueryResults(books);
            commit(conn);
            return new ApiResult(true, bookqueryresults);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        Connection conn = connector.getConn();
        int book_id = 0;
        long returnTime = 0;
        try {
            String sql="select * from borrow where card_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,borrow.getCardId());
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                book_id = rs.getInt(2);
                returnTime = rs.getLong(4);
                if((borrow.getBookId()==book_id)&&(returnTime==0)){
                    commit(conn);
                    System.out.println("错误:这本书你已经借过了" );
                    return new ApiResult(false, "you have already borrow it");
                }
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        int stock = -1;
        try {
            String sql="select stock from book where book_id=? for update;";//加了一个for update就对了
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,borrow.getBookId());
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                stock = rs.getInt(1);
            }
            if(stock==-1){
                commit(conn);
                System.out.println("错误:没有这样的书去借" );
                return new ApiResult(false, "no such book to borrow");
            }
            if(stock==0){
                commit(conn);
                System.out.println("错误:库存不足,你不能借这本书" );
                return new ApiResult(false, "no stock,you could not borrow it");
            }
            sql="update book set stock=stock-1 where book_id=?;";
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,borrow.getBookId());
            pstmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        try {
            String sql="insert into borrow(`card_id`,`book_id`,`borrow_time`,`return_time`) values (?,?,?,'0');";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,borrow.getCardId());
            pstmt.setInt(2,borrow.getBookId());
            pstmt.setLong(3,borrow.getBorrowTime());
            pstmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try {
            String sql="update borrow set return_time=? where card_id=? and book_id=? and borrow_time=? and return_time='0';";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1,borrow.getReturnTime());
            pstmt.setInt(2,borrow.getCardId());
            pstmt.setInt(3,borrow.getBookId());
            pstmt.setLong(4,borrow.getBorrowTime());
            if(pstmt.executeUpdate()==0){
                commit(conn);
                System.out.println("错误:没有这样一本书去还");
                return new ApiResult(false, "no such book to return");
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        try {
            String sql="update book set stock=stock+1 where book_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,borrow.getBookId());
            pstmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        Connection conn = connector.getConn();
        List<BorrowHistories.Item> items = new LinkedList<>();
        List<Borrow> borrows = new LinkedList<>();
        BorrowHistories borrowhistories = new BorrowHistories(items);
        try {
            String sql="select * from borrow where card_id=? order by borrow_time DESC, book_id ASC;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, cardId);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                Borrow borrow = new Borrow();
                borrow.setCardId(cardId);
                borrow.setBookId(rs.getInt(2));
                borrow.setBorrowTime(rs.getLong(3));
                borrow.setReturnTime(rs.getLong(4));
                borrows.add(borrow);
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        int i;
        for(i=0;i < borrows.size();i++){
            try {
                String sql="select * from book where book_id=?;";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, borrows.get(i).getBookId());
                ResultSet rs = pstmt.executeQuery();
                while(rs.next()){
                    Book book = new Book();
                    book.setBookId(rs.getInt(1));
                    book.setCategory(rs.getString(2));
                    book.setTitle(rs.getString(3));
                    book.setPress(rs.getString(4));
                    book.setPublishYear(rs.getInt(5));
                    book.setAuthor(rs.getString(6));
                    book.setPrice(rs.getDouble(7));
                    book.setStock(rs.getInt(8));
                    BorrowHistories.Item item = new BorrowHistories.Item(cardId,book,borrows.get(i));
                    items.add(item);
                }
                borrowhistories = new BorrowHistories(items);
                commit(conn);
            } catch (Exception e) {
                rollback(conn);
                System.out.println("Error: " + e.getMessage());
                return new ApiResult(false, e.getMessage());
            }
        }
        return new ApiResult(true,borrowhistories);
    }

    @Override
    public ApiResult registerCard(Card card) {
        Connection conn = connector.getConn();
        try {
            String sql="insert into card(`name`,`department`,`type`) values (?,?,?);";
            PreparedStatement pstmt = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1,card.getName());
            pstmt.setString(2,card.getDepartment());
            if(card.getType().getStr()=="S"){
                pstmt.setString(3, "S");
            }
            else{
                pstmt.setString(3, "T");
            }
            if(pstmt.executeUpdate()==1){
                ResultSet rs=pstmt.getGeneratedKeys();
                while(rs.next()){
                    card.setCardId(rs.getInt(1));
                }
                System.out.println("你成功注册借阅证"+card.toString());
            }
            else{
                System.out.println("注册借阅证失败");
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult removeCard(int cardId) {
        Connection conn = connector.getConn();
        int card_id = -1;
        try {
            String sql="select card_id from card where card_id=? ;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,cardId);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                card_id = rs.getInt(1);
            }
            if(card_id==(-1)){
                commit(conn);
                System.out.println("错误:没有这张借阅证" );
                return new ApiResult(false, "no such card");
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        card_id=-1;
        try {
            String sql="select card_id from borrow where card_id=? and return_time='0';";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,cardId);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                card_id = rs.getInt(1);
            }
            if(card_id!=(-1)){
                commit(conn);
                System.out.println("错误:仍有人借这本书,你不能移除它" );
                return new ApiResult(false, "someone borrow it,you can not remove!");
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        try {
            String sql="delete from card where card_id=?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,cardId);
            pstmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult showCards() {
        Connection conn = connector.getConn();
        List<Card> cards = new LinkedList<>();
        try {
            String sql="select * from card order by card_id ASC;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                Card temp = new Card();
                temp.setCardId(rs.getInt(1));
                temp.setName(rs.getString(2));
                temp.setDepartment(rs.getString(3));
                temp.setType(rs.getString(4).charAt(0)=='S'?CardType.Student:CardType.Teacher);
                cards.add(temp);
            }
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println("Error: " + e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        CardList cardlist = new CardList(cards);
        return new ApiResult(true,cardlist);
    }

    @Override
    public ApiResult resetDatabase() {
        Connection conn = connector.getConn();
        try {
            Statement stmt = conn.createStatement();
            DBInitializer initializer = connector.getConf().getType().getDbInitializer();
            stmt.addBatch(initializer.sqlDropBorrow());
            stmt.addBatch(initializer.sqlDropBook());
            stmt.addBatch(initializer.sqlDropCard());
            stmt.addBatch(initializer.sqlCreateCard());
            stmt.addBatch(initializer.sqlCreateBook());
            stmt.addBatch(initializer.sqlCreateBorrow());
            stmt.executeBatch();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
