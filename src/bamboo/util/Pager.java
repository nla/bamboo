package bamboo.util;

import droute.Request;

import java.util.List;

import static bamboo.util.Parsing.parseLongOrDefault;

public class Pager<T> {
    public final long pageSize = 100;
    public final long currentPage;
    public final long lastPage;
    public final long offset;
    public final long totalItems;
    public final List<T> items;

    public Pager(Request request, String pageParam, long totalItems, PaginationQuery<T> query) {
        currentPage = parseLongOrDefault(request.queryParam(pageParam), 1);
        offset = (currentPage - 1) * pageSize;
        this.totalItems = totalItems;
        lastPage = totalItems / pageSize + 1;
        items = query.paginate(pageSize, offset);
    }

    public interface PaginationQuery<T> {
        List<T> paginate(long limit, long offset);
    }
}
