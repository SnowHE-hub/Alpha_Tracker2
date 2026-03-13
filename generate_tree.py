import os
import sys

def build_tree(root_path):
    # 递归遍历，返回一个字符串形式的树形结构
    lines = []

    def walk(current_path, prefix=""):
        try:
            entries = sorted(os.listdir(current_path))
        except PermissionError:
            # 如果权限不足，跳过
            return

        for idx, name in enumerate(entries):
            path = os.path.join(current_path, name)
            is_dir = os.path.isdir(path)
            connector = "└── " if idx == len(entries) - 1 else "├── "
            lines.append(f"{prefix}{connector}{name}")
            if is_dir:
                # 更新前缀：最后一个分支要加空格，其他分支加竖线
                next_prefix = prefix + ("    " if idx == len(entries) - 1 else "│   ")
                walk(path, next_prefix)

    # 根节点名称
    root_name = os.path.basename(os.path.abspath(root_path.rstrip(os.sep)))
    lines.append(root_name)
    walk(root_path, "")
    return "\n".join(lines)

def main():
    if len(sys.argv) < 2:
        print("用法: python generate_tree.py <根目录路径> [输出文本文件路径]")
        sys.exit(1)

    root = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    tree_str = build_tree(root)

    # 输出到控制台
    print(tree_str)

    # 可选输出到文本文件
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(tree_str)
        print(f"\n结构树已输出到: {output_path}")

if __name__ == "__main__":
    main()


#  python generate_tree.py "D:\alpha_tracker2" "D:\alpha_tracker2\alpha_tracker2_structure.txt"