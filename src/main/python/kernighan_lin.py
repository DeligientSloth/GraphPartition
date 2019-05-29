# -*- coding: utf-8 -*-

def load_graph_data(filename: str):

    with open(filename, "r", encoding="utf-8") as f:
        edges, vertex_ids, vertex = [], [], []

        for row in f:
            left, right = [int(x) for x in row.rstrip("\n").split(',')]
            edges.append(Edge(left, right))

            if left not in vertex_ids:
                vertex_ids.append(left)
                vertex.append(Vertex(left))
            if right not in vertex_ids:
                vertex_ids.append(right)
                vertex.append(Vertex(right))

    return Graph(vertex, edges)


class Vertex(object):
    def __init__(self, vertex_id):
        self.vertex_id = vertex_id
        self.edges = []
        self.group = None

    def add_edge(self, edge):
        for edge in self.edges:
            if edge.left_id == edge.right_id and \
                    edge.right_id == edge.left_id:
                return
        self.edges.append(edge)

    def get_cost(self):
        cost = 0
        for edge in self.edges:
            if edge.left_id != self.vertex_id:
                other_vertex = edge.left_vertex
            else:
                other_vertex = edge.right_vertex

            if other_vertex.group != self.group:
                cost += 1
            else:
                cost -= 1

        return cost


class Edge(object):
    def __init__(self, left, right):
        self.left_id = left
        self.right_id = right
        self.left_vertex = None
        self.right_vertex = None

    def set_left_vertex(self, left_vertex):
        self.left_vertex = left_vertex
        left_vertex.add_edge(self)

    def set_right_vertex(self, right_vertex):
        self.right_vertex = right_vertex
        right_vertex.add_edge(self)


class Graph(object):
    def __init__(self, vertex, edges):
        self.edges = edges
        self.vertex = vertex
        self.cut_size = int(len(self.vertex) / 2)
        self.__vertex_map = {vx.vertex_id: vx for vx in self.vertex}
        self.group_a, self.group_b = [], []
        self.random_partition()
        self.__build_graph__()

    def __build_graph__(self):
        for edge in self.edges:
            edge.set_left_vertex(self.__vertex_map[edge.left_id])
            edge.set_right_vertex(self.__vertex_map[edge.right_id])

    def random_partition(self):
        for i in range(self.cut_size):
            self.vertex[i].group = 'A'
            self.group_a.append(self.vertex[i])
        for i in range(self.cut_size, len(self.vertex)):
            self.vertex[i].group = 'B'
            self.group_b.append(self.vertex[i])
        return self.group_a, self.group_b

    def get_random_groups(self):
        return self.group_a, self.group_b

    def get_groups(self):
        a_group = []
        b_group = []
        for vertex in self.vertex:
            if vertex.group == 'A':
                a_group.append(vertex)
            elif vertex.group == 'B':
                b_group.append(vertex)
        return a_group, b_group

    def get_cut_size(self):
        return self.cut_size

    def get_vertexs(self):
        return self.vertex


class KernighanLin(object):
    def __init__(self, graph):
        self.graph = graph
        self.group_a_unchosen, self.group_b_unchosen = \
            self.graph.get_random_groups()
        self.cut_size = self.graph.get_cut_size()
        self.external_cut_size = float("Inf")

    def start(self):
        """ Run Kernighan-Lin Algorithm. """

        min_id = -1
        self.swaps = []
        while self.new_external_cut_size() < self.external_cut_size:
            self.external_cut_size = self.new_external_cut_size()
            min_external_cost = float("Inf")

            for i in range(self.cut_size):
                self.single_swaps()
                cost = self.new_external_cut_size()
                if cost < min_external_cost:
                    min_external_cost = cost
                    min_id = i

            # Undo swaps done after the minimum was reached
            for i in range(min_id + 1, self.cut_size):
                vertex_b, vertex_a = self.swaps[i]
                self.do_swap(vertex_a, vertex_b)

            self.group_a_unchosen, self.group_b_unchosen = \
            self.graph.get_groups()

        self.print_result()

    def single_swaps(self):
        best_pair = None
        best_heuristic = -1 * float("Inf")

        for vertex_a in self.group_a_unchosen:
            for vertex_b in self.group_b_unchosen:
                # c_ab
                cost_edge = len(set(vertex_a.edges).intersection(vertex_b.edges))
                # g_ab = D_a + D_b - 2 * c_ab
                heuristic = vertex_a.get_cost() + vertex_b.get_cost() - 2 * cost_edge

                # Choose the best pair
                if heuristic > best_heuristic:
                    best_heuristic = heuristic
                    best_pair = vertex_a, vertex_b

        if best_pair:
            vertex_a, vertex_b = best_pair
            self.group_a_unchosen.remove(vertex_a)
            self.group_b_unchosen.remove(vertex_b)
            self.do_swap(vertex_a, vertex_b)
            self.swaps.append(best_pair)
            return best_heuristic
        else:
            raise Exception('empty maximum')

    def new_external_cut_size(self):
        cost = 0
        for edge in self.graph.edges:
            if edge.left_vertex.group != edge.right_vertex.group:
                cost += 1
        return cost

    @staticmethod
    def do_swap(vertex_a, vertex_b):
        vertex_a.group, vertex_b.group = vertex_b.group, vertex_a.group,

    def print_result(self):
        values = {v.vertex_id: v.group for v in self.graph.get_vertexs()}
        str_out = "["
        for key in values.keys():
            if values[key] == 'A':
                val = '1'
            else:
                val = '0'
            str_out += "%s ;" % val
        str_out += "]"
        print("Paste this \"state\" vector in the matlab graphviz script..  ")
        print(str_out)

        # Calculate performance
        internal_num, external_num = 0, 0
        for edge in self.graph.edges:
            if edge.left_vertex.group == None or edge.right_vertex.group == None:
                print("Fuck")
            elif edge.left_vertex.group == edge.right_vertex.group:
                internal_num += 1
            else:
                external_num += 1
        
        print(internal_num)
        print(external_num)
        print(internal_num / external_num)
        

DATA_FILE = 'data\\network.csv'

if __name__ == "__main__":
    graph = load_graph_data(DATA_FILE)
    kl = KernighanLin(graph)
    kl.start()
