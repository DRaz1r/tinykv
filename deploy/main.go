package main

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	pd "github.com/pingcap-incubator/tinykv/scheduler/client"
	"google.golang.org/grpc"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	scheduler = "tinyscheduler-server"
	tinykv    = "tinykv-server"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cluster subcommand",
		Short: "simple cluster operation tool",
	}

	nodeNumber    int
	deployPath    string
	binaryPath    string
	schedulerPort = 2379
	kvPort        = 20160

	tikvAddr  string
	tikvKey   string
	tikvValue string

	startVersionStr  string
	commitVersionStr string

	deployCmd = &cobra.Command{
		Use:   "deploy",
		Short: "deploy a local cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Build the new binaries.
			_, err := exec.Command("make", "scheduler").Output()
			if err != nil {
				log.Fatal("error happened build scheduler calling make scheduler", zap.Error(err))
			}
			_, err = exec.Command("make", "kv").Output()
			if err != nil {
				log.Fatal("error happened when build tinykv calling make kv", zap.Error(err))
			}

			// Create the deploy path.
			if _, err := os.Stat(deployPath); os.IsNotExist(err) {
				err := os.Mkdir(deployPath, os.ModePerm)
				if err != nil {
					log.Fatal("create deploy dir failed", zap.Error(err))
				}
			} else {
				log.Fatal("the dir already exists, please remove it first", zap.String("deploy path", deployPath))
			}

			// Create the scheduler and kv path, and copy new binaries to the target path.
			schedulerPath := path.Join(deployPath, "scheduler")
			err = os.Mkdir(schedulerPath, os.ModePerm)
			if err != nil {
				log.Fatal("create scheduler dir failed", zap.Error(err))
			}
			_, err = exec.Command("cp", path.Join(binaryPath, scheduler), schedulerPath).Output()
			if err != nil {
				log.Fatal("copy scheduler binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("schedulerPath", schedulerPath))
			}
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				err = os.Mkdir(kvPath, os.ModePerm)
				if err != nil {
					log.Fatal("create tinykv dir failed", zap.Error(err))
				}
				_, err = exec.Command("cp", path.Join(binaryPath, tinykv), kvPath).Output()
				if err != nil {
					log.Fatal("copy tinykv binary to path failed", zap.Error(err),
						zap.String("binaryPath", binaryPath), zap.String("tinykvPath", kvPath))
				}
			}
		},
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "start the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Try to start the scheduler server.
			log.Info("start info", zap.Int("scheduler port", schedulerPort), zap.Int("kv port", kvPort))
			_, err := Check(schedulerPort)
			if err != nil {
				log.Fatal("check scheduler port failed", zap.Error(err), zap.Int("port", schedulerPort))
			}
			schedulerPath := path.Join(deployPath, "scheduler")
			t := time.Now()
			tstr := t.Format("20060102150405")
			logName := fmt.Sprintf("log_%s", tstr)
			startSchedulerCmd := fmt.Sprintf("nohup ./%s > %s 2>&1 &", scheduler, logName)
			log.Info("start scheduler cmd", zap.String("cmd", startSchedulerCmd))
			shellCmd := exec.Command("bash", "-c", startSchedulerCmd)
			shellCmd.Dir = schedulerPath
			_, err = shellCmd.Output()
			if err != nil {
				os.Remove(path.Join(schedulerPath, logName))
				log.Fatal("start scheduler failed", zap.Error(err))
			}
			err = waitPortUse([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port in use error", zap.Error(err))
			}

			// Try to start the tinykv server.
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				thisKVPort := kvPort + i
				_, err := Check(thisKVPort)
				if err != nil {
					log.Fatal("check kv port failed", zap.Error(err), zap.Int("kv port", thisKVPort))
				}
				startKvCmd := fmt.Sprintf(`nohup ./%s > %s --path . --addr "127.0.0.1:%d" 2>&1 &`, tinykv, logName, thisKVPort)
				log.Info("start tinykv cmd", zap.String("cmd", startKvCmd))
				shellCmd := exec.Command("bash", "-c", startKvCmd)
				shellCmd.Dir = kvPath
				_, err = shellCmd.Output()
				if err != nil {
					os.Remove(path.Join(kvPath, logName))
					log.Fatal("start scheduler failed", zap.Error(err))
				}
				ports = append(ports, thisKVPort)
			}
			err = waitPortUse(ports)
			if err != nil {
				log.Fatal("wait tinykv port in use error", zap.Error(err))
			}

			log.Info("start cluster finished")
		},
	}

	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "stop the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Try to stop the tinykv server.
			killScheduler := fmt.Sprintf("pkill -f %s", scheduler)
			log.Info("start scheduler cmd", zap.String("cmd", killScheduler))
			shellCmd := exec.Command("bash", "-c", killScheduler)
			_, err := shellCmd.Output()
			if err != nil {
				log.Fatal("stop scheduler failed", zap.Error(err))
			}
			err = waitPortFree([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port free error", zap.Error(err))
			}

			// Try to stop the scheduler server.
			killKvServer := fmt.Sprintf("pkill -f %s", tinykv)
			log.Info("start scheduler cmd", zap.String("cmd", killKvServer))
			shellCmd = exec.Command("bash", "-c", killKvServer)
			_, err = shellCmd.Output()
			if err != nil {
				log.Fatal("stop tinykv failed", zap.Error(err))
			}
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				thisKVPort := kvPort + i
				ports = append(ports, thisKVPort)
			}
			err = waitPortFree(ports)
			if err != nil {
				log.Fatal("wait tinykv port free error", zap.Error(err))
			}
		},
	}

	upgradeCmd = &cobra.Command{
		Use:   "upgrade",
		Short: "upgrade the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Check the cluster is stopped.
			err := waitPortFree([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port free error", zap.Error(err))
			}
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				thisKVPort := kvPort + i
				ports = append(ports, thisKVPort)
			}
			err = waitPortFree(ports)
			if err != nil {
				log.Fatal("wait tinykv port free error", zap.Error(err))
			}

			// Rebuild the binary.
			_, err = exec.Command("make", "scheduler").Output()
			if err != nil {
				log.Fatal("error happened build scheduler calling make scheduler", zap.Error(err))
			}
			_, err = exec.Command("make", "kv").Output()
			if err != nil {
				log.Fatal("error happened when build tinykv calling make kv", zap.Error(err))
			}

			// Substitute the binary.
			schedulerPath := path.Join(deployPath, "scheduler")
			_, err = exec.Command("cp", path.Join(binaryPath, scheduler), schedulerPath).Output()
			if err != nil {
				log.Fatal("copy scheduler binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("schedulerPath", schedulerPath))
			}
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				_, err = exec.Command("cp", path.Join(binaryPath, tinykv), kvPath).Output()
				if err != nil {
					log.Fatal("copy tinykv binary to path failed", zap.Error(err),
						zap.String("binaryPath", binaryPath), zap.String("kvPath", kvPath))
				}
			}
		},
	}

	destroyCmd = &cobra.Command{
		Use:   "destroy",
		Short: "destroy the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("destroy is starting to remove the whole deployed directory",
				zap.String("deployPath", deployPath))
			err := os.RemoveAll(deployPath)
			if err != nil {
				log.Fatal("remove the deploy path failed", zap.Error(err))
			}
			log.Info("cleanup finished")
		},
	}

	// 新增获取节点信息的命令
	getNodesCmd = &cobra.Command{
		Use:   "get-nodes",
		Short: "get all tinykv nodes info",
		Run: func(cmd *cobra.Command, args []string) {
			// 实例化 Client
			pdAddrs := []string{"127.0.0.1:2379"}
			security := pd.SecurityOption{
				CAPath:   "",
				CertPath: "",
				KeyPath:  "",
			}
			client, err := pd.NewClient(pdAddrs, security)
			if err != nil {
				log.Fatal("failed to create PD client", zap.Error(err))
			}
			defer client.Close()

			ctx := context.Background()
			err = GetAllTinyKVNodesInfo(client, ctx)
			if err != nil {
				log.Fatal("failed to get nodes info", zap.Error(err))
			}
		},
	}
	// 定义 get 命令
	getCmd = &cobra.Command{
		Use:   "get",
		Short: "Get a key from TinyKV",
		Run: func(cmd *cobra.Command, args []string) {
			// 解析命令行参数
			addr := tikvAddr
			key := tikvKey

			// 建立 gRPC 连接
			conn, err := grpc.Dial(addr, grpc.WithInsecure(),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:    3 * time.Second,
					Timeout: 60 * time.Second,
				}))
			if err != nil {
				log.Fatal("Failed to connect", zap.Error(err))
			}
			defer conn.Close()

			client := tinykvpb.NewTinyKvClient(conn)

			// 执行 get 命令
			value, err := get(client, key)
			if err != nil {
				log.Fatal("Failed to get key", zap.Error(err))
			} else {
				fmt.Printf("Value: %s\n", value)
			}
		},
	}

	// 定义 set 命令
	setCmd = &cobra.Command{
		Use:   "set",
		Short: "Set a key-value pair in TinyKV",
		Run: func(cmd *cobra.Command, args []string) {
			// 解析命令行参数
			addr := tikvAddr
			key := tikvKey
			value := tikvValue

			// 建立 gRPC 连接
			conn, err := grpc.Dial(addr, grpc.WithInsecure(),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:    3 * time.Second,
					Timeout: 60 * time.Second,
				}))
			if err != nil {
				log.Fatal("Failed to connect", zap.Error(err))
			}
			defer conn.Close()

			client := tinykvpb.NewTinyKvClient(conn)

			// 执行 set 命令
			err = set(client, key, value)
			if err != nil {
				log.Fatal("Failed to set key", zap.Error(err))
			}
			fmt.Println("Key set successfully")
		},
	}

	getByTxnCmd = &cobra.Command{
		Use:   "getByTxn [key]",
		Short: "Get a value by transaction",
		Long:  "Get a value from the database using a transaction.",
		Run: func(cmd *cobra.Command, args []string) {
			// 解析命令行参数
			addr := tikvAddr
			key := tikvKey

			// 建立 gRPC 连接
			conn, err := grpc.Dial(addr, grpc.WithInsecure(),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:    3 * time.Second,
					Timeout: 60 * time.Second,
				}))
			if err != nil {
				log.Fatal("Failed to connect", zap.Error(err))
			}
			defer conn.Close()

			client := tinykvpb.NewTinyKvClient(conn)

			value, err := getByTxn(client, key)
			if err != nil {
				log.Fatal("Failed to get value by transaction", zap.Error(err))
			}
			fmt.Printf("Value: %s\n", value)
		},
	}

	setByTxnCmd = &cobra.Command{
		Use:   "setByTxn [key] [value]",
		Short: "Set a value by transaction",
		Long:  "Set a value in the database using a transaction.",
		Run: func(cmd *cobra.Command, args []string) {
			addr := tikvAddr
			key := tikvKey
			value := tikvValue

			// 建立 gRPC 连接
			conn, err := grpc.Dial(addr, grpc.WithInsecure(),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:    3 * time.Second,
					Timeout: 60 * time.Second,
				}))
			if err != nil {
				log.Fatal("Failed to connect", zap.Error(err))
			}
			defer conn.Close()

			client := tinykvpb.NewTinyKvClient(conn)

			err = setByTxn(client, key, value)
			if err != nil {
				log.Fatal("Failed to set key", zap.Error(err))
			}
			fmt.Println("Key set successfully")
		},
	}

	scaleOutCmd = &cobra.Command{
		Use:   "scale-out",
		Short: "Scale out the cluster by adding a new tinykv node",
		Run: func(cmd *cobra.Command, args []string) {
			// 确定新节点的编号
			newNodeNumber := nodeNumber
			newKVPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", newNodeNumber))

			// 创建新节点的目录
			err := os.Mkdir(newKVPath, os.ModePerm)
			if err != nil {
				log.Fatal("create new tinykv dir failed", zap.Error(err))
			}

			// 复制新的 tinykv 二进制文件到新节点目录
			_, err = exec.Command("cp", path.Join(binaryPath, tinykv), newKVPath).Output()
			if err != nil {
				log.Fatal("copy tinykv binary to new path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("newKVPath", newKVPath))
			}

			// 计算新节点的端口
			newKVPort := kvPort + newNodeNumber

			// 检查端口是否可用
			_, err = Check(newKVPort)
			if err != nil {
				log.Fatal("check new kv port failed", zap.Error(err), zap.Int("new kv port", newKVPort))
			}

			// 启动新节点
			t := time.Now()
			tstr := t.Format("20060102150405")
			logName := fmt.Sprintf("log_%s", tstr)
			startKvCmd := fmt.Sprintf(`nohup ./%s > %s --path . --addr "127.0.0.1:%d" 2>&1 &`, tinykv, logName, newKVPort)
			log.Info("start new tinykv cmd", zap.String("cmd", startKvCmd))
			shellCmd := exec.Command("bash", "-c", startKvCmd)
			shellCmd.Dir = newKVPath
			_, err = shellCmd.Output()
			if err != nil {
				os.Remove(path.Join(newKVPath, logName))
				log.Fatal("start new tinykv failed", zap.Error(err))
			}

			// 等待端口被使用
			err = waitPortUse([]int{newKVPort})
			if err != nil {
				log.Fatal("wait new tinykv port in use error", zap.Error(err))
			}

			// 增加节点数量
			nodeNumber++

			log.Info("scale out cluster finished")
		},
	}
)

func getContextForSet() (kvrpcpb.Context, error) {
	retCtx := kvrpcpb.Context{}
	var (
		storeID     uint64
		peerID      uint64
		regionID    uint64
		regionEpoch *metapb.RegionEpoch
	)

	pdAddrs := []string{"127.0.0.1:2379"}
	security := pd.SecurityOption{
		CAPath:   "",
		CertPath: "",
		KeyPath:  "",
	}
	pdClient, err := pd.NewClient(pdAddrs, security)
	if err != nil {
		log.Fatal("failed to create PD client", zap.Error(err))
		return retCtx, err
	}
	defer pdClient.Close()

	stores, err := pdClient.GetAllStores(context.Background())
	if err != nil {
		return retCtx, fmt.Errorf("failed to get all stores: %v", err)
	}

	for _, store := range stores {
		if store.GetAddress() == tikvAddr {
			storeID = store.GetId()
		}
	}

	stkey := []byte{}
	endKey := []byte{}
	regions, leaderPeers, err := pdClient.ScanRegions(context.Background(), stkey, endKey, 0)
	if err != nil {
		return retCtx, err
	}
	for i, region := range regions {
		leaderPeer := leaderPeers[i]
		peers := region.GetPeers()
		for _, peer := range peers {
			if peer.GetStoreId() == storeID && peer.GetId() == leaderPeer.GetId() {
				peerID = peer.GetId()
				regionID = region.GetId()
				regionEpoch = region.GetRegionEpoch()
			}
		}
	}
	if peerID == 0 {
		return retCtx, fmt.Errorf("no leader peer found with store ID %d address %s", storeID, tikvAddr)
	}
	retCtx = kvrpcpb.Context{
		RegionId:    regionID,
		RegionEpoch: regionEpoch,
		Peer: &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		},
	}
	return retCtx, nil
}

func getContextForGet(key string) (kvrpcpb.Context, error) {
	retCtx := kvrpcpb.Context{}
	var storeID uint64
	var peerID uint64

	pdAddrs := []string{"127.0.0.1:2379"}
	security := pd.SecurityOption{
		CAPath:   "",
		CertPath: "",
		KeyPath:  "",
	}
	pdClient, err := pd.NewClient(pdAddrs, security)
	if err != nil {
		log.Fatal("failed to create PD client", zap.Error(err))
		return retCtx, err
	}
	defer pdClient.Close()

	stores, err := pdClient.GetAllStores(context.Background())
	if err != nil {
		return retCtx, fmt.Errorf("failed to get all stores: %v", err)
	}

	for _, store := range stores {
		if store.GetAddress() == tikvAddr {
			storeID = store.GetId()
		}
	}

	region, peer, err := pdClient.GetRegion(context.Background(), []byte(key))
	if err != nil {
		return retCtx, fmt.Errorf("failed to get region by key %s: %v", key, err)
	}
	if region == nil || peer == nil {
		return retCtx, fmt.Errorf("no region found with key %s", key)
	}
	for _, peer := range region.Peers {
		if peer.GetStoreId() == storeID {
			peerID = peer.GetId()
		}
	}
	retCtx = kvrpcpb.Context{
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		Peer: &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		},
	}
	return retCtx, nil
}

// get 函数调用 TinyKV 的 RawGet 接口获取键值
func get(client tinykvpb.TinyKvClient, key string) (string, error) {
	ctx1, err := getContextForGet(key)
	if err != nil {
		return "", err
	}
	req := &kvrpcpb.RawGetRequest{
		Context: &ctx1,
		Key:     []byte(key),
		Cf:      "default",
	}
	ctx := context.Background()
	resp, err := client.RawGet(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.NotFound {
		return "", errors.New("key not found")
	} else if len(resp.Error) != 0 {
		return "", errors.New(resp.Error)
	} else if resp.RegionError != nil {
		return "", errors.New(string(resp.RegionError.String()))
	}
	return string(resp.Value), nil
}

// set 函数调用 TinyKV 的 RawPut 接口设置键值
func set(client tinykvpb.TinyKvClient, key, value string) error {
	ctx1, err := getContextForSet()
	if err != nil {
		return err
	}
	req := &kvrpcpb.RawPutRequest{
		Context: &ctx1,
		Key:     []byte(key),
		Value:   []byte(value),
		Cf:      "default",
	}
	ctx := context.Background()
	resp, err := client.RawPut(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.Error) != 0 {
		return errors.New(resp.Error)
	}
	if resp.RegionError != nil {
		return errors.New(resp.RegionError.String())
	}
	return nil
}

func getByTxn(client tinykvpb.TinyKvClient, key string) (string, error) {
	ctx1, err := getContextForGet(key)
	if err != nil {
		return "", err
	}
	commitVersion, _ := strconv.ParseUint(commitVersionStr, 10, 64)
	// 准备 KvGet 请求
	getReq := &kvrpcpb.GetRequest{
		Context: &ctx1,
		Key:     []byte(key),
		Version: commitVersion,
	}

	// 调用 KvGet
	getResp, err := client.KvGet(context.Background(), getReq)
	if err != nil {
		return "", err
	}
	if getResp.Error != nil {
		return "", errors.New(getResp.Error.String())
	}
	if getResp.NotFound {
		return "", errors.New("key not found")
	}
	if getResp.RegionError != nil {
		return "", errors.New(getResp.RegionError.String())
	}
	return string(getResp.Value), nil
}

func setByTxn(client tinykvpb.TinyKvClient, key, value string) error {
	ctx1, err := getContextForSet()
	if err != nil {
		return err
	}
	// 初始化请求上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 定义事务相关参数
	startVersion, _ := strconv.ParseUint(startVersionStr, 10, 64)
	commitVersion, _ := strconv.ParseUint(commitVersionStr, 10, 64)

	primaryKey := []byte("primary_key")
	k := []byte(key)
	v := []byte(value)

	// 准备 KvPrewrite 请求
	mutation := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_Put,
		Key:   k,
		Value: v,
	}
	prewriteReq := &kvrpcpb.PrewriteRequest{
		// 这里需要根据实际情况填充 Context，示例中使用空值
		Context:      &ctx1,
		Mutations:    []*kvrpcpb.Mutation{mutation},
		PrimaryLock:  primaryKey,
		StartVersion: startVersion,
		LockTtl:      100000, // 锁的生存时间，单位毫秒
	}
	time.Sleep(10000)
	// 调用 KvPrewrite
	prewriteResp, err := client.KvPrewrite(ctx, prewriteReq)
	if err != nil {
		return err
	}
	if len(prewriteResp.Errors) > 0 {
		return errors.New(prewriteResp.Errors[0].String())
	}
	fmt.Println("KvPrewrite 成功")

	// 准备 KvCommit 请求
	commitReq := &kvrpcpb.CommitRequest{
		Context:       &ctx1,
		StartVersion:  startVersion,
		Keys:          [][]byte{[]byte(key)},
		CommitVersion: commitVersion,
	}

	// 调用 KvCommit
	commitResp, err := client.KvCommit(ctx, commitReq)
	if err != nil {
		return err
	}
	if commitResp.Error != nil {
		return errors.New(commitResp.Error.String())
	}
	fmt.Println("通过事务写入成功")
	return nil
}

// 获取所有节点信息的函数
func GetAllTinyKVNodesInfo(client pd.Client, ctx context.Context) error {
	// 获取所有的存储节点信息
	stores, err := client.GetAllStores(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all stores: %v", err)
	}

	for _, store := range stores {
		fmt.Printf("Store ID: %d, Address: %s\n", store.GetId(), store.GetAddress())
	}
	// 扫描每个存储节点下的区域（Region）和对应的 peer
	key := []byte{}
	endKey := []byte{}
	regions, leaderPeers, err := client.ScanRegions(ctx, key, endKey, 0)

	for i, region := range regions {
		fmt.Printf("  Region ID: %d, Region Start Key: %s\n", region.GetId(), region.GetStartKey())
		peer := leaderPeers[i]
		fmt.Printf("    Leader Peer ID: %d\n", peer.GetId())
		// 可以进一步处理 peer 信息，例如获取其他 peer 等
		peers := region.GetPeers()
		for _, peer := range peers {
			fmt.Printf("    Peer ID: %d In Store: %d\n", peer.GetId(), peer.GetStoreId())
		}
	}

	return nil
}

// Check if a port is available
func Check(port int) (status bool, err error) {

	// Concatenate a colon and the port
	host := ":" + strconv.Itoa(port)

	// Try to create a server with the port
	server, err := net.Listen("tcp", host)

	// if it fails then the port is likely taken
	if err != nil {
		return false, err
	}

	// close the server
	server.Close()

	// we successfully used and closed the port
	// so it's now available to be used again
	return true, nil

}

const waitDurationLimit = time.Duration(2 * time.Minute)
const waitSleepDuration = time.Duration(300 * time.Millisecond)

func checkLoop(waitPorts []int, checkFn func([]int) bool) error {
	waitStart := time.Now()
	for {
		if time.Since(waitStart) > waitDurationLimit {
			log.Error("check port free timeout")
			return errors.New("check port free timeout")
		}
		if checkFn(waitPorts) {
			break
		}
		time.Sleep(waitSleepDuration)
	}
	return nil
}

func waitPortFree(waitPorts []int) error {
	return checkLoop(waitPorts, func(ports []int) bool {
		allFree := true
		for _, port := range waitPorts {
			_, err := Check(port)
			if err != nil {
				allFree = false
				break
			}
		}
		return allFree
	})
}

func waitPortUse(waitPorts []int) error {
	return checkLoop(waitPorts, func(ports []int) bool {
		allInUse := true
		for _, port := range waitPorts {
			_, err := Check(port)
			if err == nil {
				allInUse = false
				break
			}
		}
		return allInUse
	})
}

func init() {
	rootCmd.Flags().IntVarP(&nodeNumber, "num", "n", 3, "the number of the tinykv servers")
	rootCmd.Flags().StringVarP(&deployPath, "deploy_path", "d", "./bin/deploy", "the deploy path")
	rootCmd.Flags().StringVarP(&binaryPath, "binary_path", "b", "./bin", "the binary path")

	rootCmd.AddCommand(deployCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(stopCmd)
	rootCmd.AddCommand(upgradeCmd)
	rootCmd.AddCommand(destroyCmd)
	rootCmd.AddCommand(getNodesCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(setCmd)
	rootCmd.AddCommand(scaleOutCmd)
	rootCmd.AddCommand(setByTxnCmd)
	rootCmd.AddCommand(getByTxnCmd)

	getCmd.Flags().StringVarP(&tikvAddr, "tikv_addr", "", "127.0.0.1:20160", "TinyKV server address")
	setCmd.Flags().StringVarP(&tikvAddr, "tikv_addr", "", "127.0.0.1:20160", "TinyKV server address")
	getByTxnCmd.Flags().StringVarP(&tikvAddr, "tikv_addr", "", "127.0.0.1:20160", "TinyKV server address")
	setByTxnCmd.Flags().StringVarP(&tikvAddr, "tikv_addr", "", "127.0.0.1:20160", "TinyKV server address")

	getCmd.Flags().StringVarP(&tikvKey, "key", "", "", "Key")
	setCmd.Flags().StringVarP(&tikvKey, "key", "", "", "Key")
	getByTxnCmd.Flags().StringVarP(&tikvKey, "key", "", "", "Key")
	setByTxnCmd.Flags().StringVarP(&tikvKey, "key", "", "", "Key")

	setCmd.Flags().StringVarP(&tikvValue, "value", "", "", "Value")
	setByTxnCmd.Flags().StringVarP(&tikvValue, "value", "", "", "Value")

	setByTxnCmd.Flags().StringVarP(&commitVersionStr, "commitVersion", "", "", "commitVersion")
	setByTxnCmd.Flags().StringVarP(&startVersionStr, "startVersion", "", "", "startVersion")

	getByTxnCmd.Flags().StringVarP(&commitVersionStr, "commitVersion", "", "", "commitVersion")
}

func main() {
	rootCmd.Execute()
}
