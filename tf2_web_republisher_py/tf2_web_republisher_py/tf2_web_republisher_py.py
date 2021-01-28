import rclpy
from rclpy.node import Node
from rclpy.action import ActionServer
from rclpy.callback_groups import ReentrantCallbackGroup
from tf2_web_republisher_msgs.msg import TFArray
from tf2_web_republisher_msgs.srv import RepublishTFs
from tf2_web_republisher_msgs.action import TFSubscription
from geometry_msgs.msg import TransformStamped
from tf2_web_republisher_py.tf_pair import TFPair
from tf2_ros import Buffer, TransformListener, TransformException

from typing import List
from uuid import uuid4

class ClientInfo(object):
    def __init__(self,client_id:int,tf_subscriptions:List[TFPair]=[],timer=None):
        self.client_id = client_id
        self.tf_subscriptions = tf_subscriptions
        self.timer = timer

class ClientGoalInfo(ClientInfo):
    def __init__(self,handle,client_id:int,tf_subscriptions:List[TFPair]=[],timer=None):
        super(ClientGoalInfo,self).__init__(client_id,tf_subscriptions,timer)
        self.handle = handle

class ClientRequestInfo(ClientInfo):
    def __init__(self,client_id:int,tf_subscriptions:List[TFPair]=[],publisher=None,timer=None,timeout:float=10.0):
        super(ClientRequestInfo,self).__init__(client_id,tf_subscriptions,timer)
        self.publisher = publisher
        self.timeout = timeout
        self.last_time_since_subscribers = 0

class TFRepublisher(Node):
    '''
    Python port of the TFRepublisher C++ Node for ROS2
    '''
    def __init__(self):
        super(TFRepublisher,self).__init__('tf2_web_republisher')
        self.tf_transform_server = ActionServer(node=self,
                                                action_type=TFSubscription,
                                                action_name='tf2_web_republisher',
                                                execute_callback=self.goal_cb,
                                                cancel_callback=self.cancel_cb)
        self.republish_service = self.create_service(RepublishTFs, 'republish_tfs', self.request_cb)
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer,node=self)
        self.active_clients = {}

    def goal_cb(self,goal_handle):
        goal = goal_handle.request
        client_id = str(uuid4())
        goal_info = ClientGoalInfo(goal_handle,client_id)
        self.set_subscriptions(goal_info,
                               goal.source_frames,
                               goal.target_frames,
                               goal.angular_thres,
                               goal.trans_thres)
        goal_info.timer = self.create_timer(1.0/goal.rate,lambda: self.process_goal(client_id))
        self.active_clients[client_id] = goal_info

    def cancel_cb(self,goal_handle):
        for client_id, goal_info in self.active_clients.items():
            if goal_handle == goal_info.handle:
                self.teardown_client(client_id)

    def request_cb(self,request,response):
        self.get_logger().info("RepublishTF service request received");
        client_id = str(uuid4())
        topic_name = 'tf_repub_'+client_id

        pub = self.create_publisher(TFArray,topic_name,10)
        # generate request_info struct
        request_info = ClientRequestInfo(pub,client_id)
        self.set_subscriptions(request_info,
                               request.source_frames,
                               request.target_frames,
                               request.angular_thres,
                               request.trans_thres)
        request_info.timeout = request.timeout
        request_info.timer = self.create_timer(1.0/request.rate,lambda: self.process_request(client_id))
        self.active_clients[client_id] = request_info
        result.topic_name = topic_name
        self.get_logger().info('Publishing requested TFs on topic {0}'.format(topic_name))
        return response

    @staticmethod
    def clean_tf_frame(frame_id:str) -> str:
        if frame_id[0] != '/':
            frame_id = '/'+frame_id
        return frame_id

    def set_subscriptions(self,client_info:ClientInfo,source_frames:List[str],target_frame:str,angular_thres:float,trans_tres:float):
        client_info.tf_subscriptions = []
        for source_frame in source_frames:
            tf_pair = TFPair(self.clean_tf_frame(source_frame),self.clean_tf_frame(target_frame),angular_thres,trans_thres)
            client_info.tf_subscriptions.append(tf_pair)

    def teardown_client(self,client_id):
        client = self.active_clients[client_id]
        client.timer.destroy()
        if isinstance(client,ClientRequestInfo):
            client.publisher.destroy()
        elif isinstance(client,ClientGoalInfo):
            client.handle.succeed()
        del self.active_clients[client_id]

    def process_request(self, client_id:str):
        request = self.active_clients[client_id]
        # Check whether the publisher has any subscribers. If it does, update the stored time.
        if request.pub.get_subscription_count() > 0:
            request.last_time_since_subscribers = self.get_clock().now()
        elif self.get_clock().now() - request.last_time_since_subscribers > request.timeout:
            self.teardown_client(client_id)
            return
        # Continue processing the request
        tf_array = self.update_subscriptions(request.tf_subscriptions)
        if len(tf_array) > 0:
            # publish TFs
            self.get_logger().debug('Request {0} TFs published:'.format(request.client_id))
            request.publisher.publish(TFArray(transforms=tf_array))
        else:
            self.get_logger().debug('Request {0} No TF frame update needed:'.format(request.client_id))

    def process_goal(self, client_id:str):
        goal = self.active_clients[client_id]
        feedback = goal.handle.Feedback()

        # Continue processing the request
        tf_array = self.update_subscriptions(request.tf_subscriptions)
        if len(tf_array) > 0:
            # publish TFs
            self.get_logger().debug('Client {0} TFs feedback published:'.format(request.client_id))
            feedback.transforms = TFArray(transforms=tf_array)
            goal.handle.publish_feedback(feedback)
        else:
            self.get_logger().debug('Client {0}: No TF frame update needed:'.format(request.client_id))

    def update_subscriptions(self,tf_subscriptions:List[TFPair]) -> List[TransformStamped]:
        transforms = []

        # iterate over tf_subscription vector
        for pair in tf_subscriptions:
            try:
                # lookup transformation for tf_pair
                transform = self.tf_buffer.lookupTransform(pair.target_frame,pair.source_frame,0)
                # If the transform broke earlier, but worked now (we didn't get
                # booted into the catch block), tell the user all is well again
                if not pair.is_ok:
                    self.get_logger().info('Transform from {0} to {1} is working again'.format(pair.source_frame,pair.target_frame))
                    pair.is_ok = True
                # update tf_pair with transformtion
                pair.update_transform(transform)

            except Exception as e:
                if pair.is_ok:
                    # Only log an error if the transform was okay before
                    self.get_logger().warning(str(e))
                pair.is_ok = False

            # check angular and translational thresholds
            if pair.needs_update:
                transform.header.stamp = self.get_clock().now().to_msg()
                transform.header.frame_id = pair.target_frame
                transform.child_frame_id = pair.source_frame
                pair.transmission_triggered()

            transforms.append(transform)

        return transforms

def main(args=[]):
    rclpy.init(args=args)

    tf_republisher_node = TFRepublisher()

    rclpy.spin(tf_republisher_node)


if __name__ == '__main__':
    main()
