package es.ulpgc.Indexer;

import java.util.*;


public class Trie {
    private final TrieNode root;

    public Trie() {
        this.root = new TrieNode();
    }

    public void insert(String word, String ebookNumber) {
        TrieNode node = root;
        for (char ch : word.toCharArray()) {
            node.children.putIfAbsent(ch, new TrieNode());
            node = node.children.get(ch);
        }
        node.ebookNumbers.add(ebookNumber);
    }
    public Set<String> search(String word) {
        TrieNode node = root;
        for (char ch : word.toCharArray()) {
            node = node.children.get(ch);
            if (node == null) {
                return Collections.emptySet();
            }
        }
        return node.ebookNumbers;
    }
    public static class TrieNode {
        Map<Character, TrieNode> children;
        Set<String> ebookNumbers;

        public TrieNode() {
            this.children = new HashMap<>();
            this.ebookNumbers = new HashSet<>();
        }
    }

}
