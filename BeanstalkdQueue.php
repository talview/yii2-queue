<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue;
use Pheanstalk\Job;
use Pheanstalk\PheanstalkInterface;
use Pheanstalk\Pheanstalk;
use Yii;
use yii\base\Component;
use yii\base\InvalidConfigException;
use yii\helpers\Json;

/**
 * Beanstalkd Queue 
 *
 * @author Mani Ka <subramanian.kailash@gmail.com>
 */
class BeanstalkdQueue extends Component implements QueueInterface
{
   /**
     * @var \Pheanstalk\Pheanstalk $beanstalkd
     */
    public $beanstalkd;
    /**
     * @var string
     */
    public $host        = 'localhost';
    /**
     * @var int
     */
    public $port        = PheanstalkInterface::DEFAULT_PORT;
    /**
     * @var int
     */
    public $timeout     = PheanstalkInterface::DEFAULT_TTR;

    /**
     * Loads the Pheanstalk client for beanstalkd message broker / queue
     * from configuration
     *
     * @throws \yii\base\InvalidConfigException
     */
    public function init()
    {
        parent::init();
        if(!$this->beanstalkd instanceof Pheanstalk){
            $this->beanstalkd = new Pheanstalk($this->host,$this->port,$this->timeout);
        }
    }

    /**
     * @param array $message
     * Deletes a job from
     * @return $this
     * @throws \yii\base\InvalidConfigException
     */
    public function delete(Array $message)
    {
        $this->validateMessage($message);
        return $this->beanstalkd->useTube($message['queue'])->delete(new Job($message['id'], $message['body']));
    }

    public function push($payload, $queue,$delay = PheanstalkInterface::DEFAULT_DELAY)
    {
        return $this->beanstalkd->putInTube(
            $queue,
            is_string($payload) ? $payload : Json::encode($payload),
            PheanstalkInterface::DEFAULT_PRIORITY,
            $delay,
            PheanstalkInterface::DEFAULT_TTR);
    }

    public function pop($queue)
    {
        $job = $this->beanstalkd->reserveFromTube($queue,$this->timeout);
        return [
            'id'    => $job->getId(),
            'queue' => $queue,
            'body'  => $job->getData()
        ];
    }

    public function release(Array $message,$delay=PheanstalkInterface::DEFAULT_DELAY)
    {
        $this->validateMessage($message);
        return $this->beanstalkd->useTube($message['queue'])
            ->release(
                new Job($message['id'],$message['body']),
                PheanstalkInterface::DEFAULT_PRIORITY,$delay
            );
    }

    public function purge($queue)
    {
        while ($job = $this->beanstalkd->watch($queue)->ignore("default")->reserve(0)) {
            $this->beanstalkd->delete($job);
        }
    }

    private function validateMessage(Array $message)
    {
        if(!isset($message['id']) || !isset($message['body']) || !isset($message['queue'])){
            throw  new InvalidConfigException("Invalid message configuration");
        }
    }
}
