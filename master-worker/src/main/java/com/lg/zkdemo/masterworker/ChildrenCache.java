package com.lg.zkdemo.masterworker;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuguo on 2017/5/26.
 */
public class ChildrenCache {

    private List<String> children;

    ChildrenCache(){
        this.children = null;
    }

    ChildrenCache(List<String> children){
        this.children = children;
    }

    public List<String> getChildren() {
        return children;
    }

    /**
     * 获取新添加的child,并设置新的children
     * @param newChildren
     * @return
     */
    List<String> addedAndSet( List<String> newChildren) {
        List<String> diff = null;

        if(children == null) {
            diff = new ArrayList<>(newChildren);
        } else {
            for(String s: newChildren) {
                if(!children.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<>();
                    }

                    diff.add(s);
                }
            }
        }
        this.children = newChildren;

        return diff;
    }

    /**
     * 获取被移除了的child,并设置新的children
     * @param newChildren
     * @return
     */
    List<String> removedAndSet( List<String> newChildren) {
        List<String> diff = null;

        if(children != null) {
            for(String s: children) {
                if(!newChildren.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<>();
                    }

                    diff.add(s);
                }
            }
        }
        this.children = newChildren;

        return diff;
    }


}
