package mydb.storage.evict;

import mydb.storage.PageId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LRU(Less Recent Used)驱逐策略
 * 最近最少使用的页面会被驱逐
 */
public class LRU implements EvictStrategy {

    /**
     * 存放页面ID的结点，用于组织成链表，便于访问Map
     */
    class PageNode {
        PageId pid;
        PageNode prev;
        PageNode next;

        public PageNode() {
            this.prev = null;
            this.next = null;
            this.pid = null;
        }

        public PageNode(PageId pid) {
            this.prev = null;
            this.next = null;
            this.pid = pid;
        }

        public PageId getPageId() {
            return pid;
        }
    }

    /**
     * 头节点，该结点的下一个结点页面最近使用过，很可能再次被访问。
     * 根据局部性原理，这种页面不应该被驱逐
     * 将head的下一个结点才用于表示页面可以避免过多null判断（类比带头结点）
     */
    PageNode head;

    /**
     * 尾节点，该结点的上一个结点的页面最近最少使用，考虑被驱逐
     * 将tail的上一个结点才用于表示页面可以避免过多null判断断（类比带头结点）
     */
    PageNode tail;

    private Map<PageId, PageNode> pageNodeMap;

    public LRU(int pagesNum) {
        head = new PageNode();
        tail = new PageNode();
        head.next = tail;
        tail.prev = head;
        pageNodeMap = new ConcurrentHashMap<>(pagesNum);
    }

    private boolean hasPageNode(PageNode node) {
        return pageNodeMap.containsKey(node.pid);
    }

    private void addToFirst(PageNode node) {
        if (!hasPageNode(node)) {
            pageNodeMap.put(node.pid, node);
        }
        head.next.prev = node;
        node.next = head.next;
        head.next = node;
        node.prev = head;
    }

    private void moveToFirst(PageNode node) {
        removeNode(node);
        addToFirst(node);
    }

    private void removeNode(PageNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        pageNodeMap.remove(node.pid);
    }

    private void removeTail() {
        PageNode node = tail.prev;
        removeNode(node);
    }

    /**
     * 页面数据修改后要将该页面结点移至first，即该页面最近使用过
     * @param pid 进行修改的页面ID
     */
    @Override
    public void modifyData(PageId pid) {
        if (pageNodeMap.containsKey(pid)) {
            PageNode pageNode = pageNodeMap.get(pid);
            moveToFirst(pageNode);
        } else {
            PageNode node = new PageNode(pid);
            pageNodeMap.put(pid, node);
            addToFirst(node);
        }
    }

    /**
     * 返回需要被驱逐的页面的ID，并将其页面结点从Map中删去
     * @return 返回需要被驱逐的页面的ID
     */
    @Override
    public PageId getEvictPageId() {
        PageNode node = tail.prev;
        removeTail();
        return node.pid;
    }
}
